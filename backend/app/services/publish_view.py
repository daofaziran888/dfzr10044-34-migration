"""消费视图领域服务：发布、刷新、撤回面向下游的视图，绑定快照与结构版本。

刷新不是单纯把绑定快照指向“最新”，而是按已确认的存量清单解析出实际读取的
逻辑文件位置版本，并把这份位置版本记录到读取回执里。迁移回执迟到时，
回执可以回接到这次实际读取上归账，不会重复计节省字节。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import ViewReadRecord, lake_store


class PublishViewService(BaseModuleService):
    MODULE = "publish_view"
    ENTITY = "消费视图"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["视图编号", "所属湖表", "视图定义", "绑定快照", "可见列", "刷新方式", "下游消费方", "最近刷新"]
    REQUIRED_FIELDS = ["视图编号", "所属湖表", "视图定义"]
    STATUS_ORDER = ["草稿", "已发布", "刷新中", "已撤回"]
    ACTION_RULES = {"发布视图": "已发布", "触发刷新": "刷新中", "撤回视图": "已撤回"}
    NEGATIVE_ACTIONS = []

    def _available_snapshots(self, lake_table: str) -> list[Any]:
        return [
            s for s in lake_store.list_snapshots(lake_table)
            if s.status == "available"
        ]

    def _confirmed_manifest(self, lake_table: str):
        """消费侧只读取已确认的清单版本。

        清单在迁移流水线发布后即为 current；若有未确认/已取消的迁移指向该清单，
        则回落到上一版已确认清单，避免读到半份位置。
        """
        active = lake_store.get_active_manifest(lake_table)
        if active is None:
            return None
        migrations = lake_store.migrations_for_manifest(lake_table, active.version_id)
        if migrations and all(m.state == "canceled" for m in migrations):
            previous = (
                lake_store.get_manifest_version(lake_table, active.previous_version_id)
                if active.previous_version_id
                else None
            )
            return previous or active
        return active

    def publish_view(self, lake_table: str, view_definition: str,
                     visible_columns: list[str], consumer: str) -> dict[str, Any]:
        """发布消费视图：绑定最新可用快照与可见列。"""
        available = self._available_snapshots(lake_table)
        latest_snap = available[-1] if available else None
        return {
            "lake_table": lake_table,
            "view_definition": view_definition,
            "bound_snapshot": latest_snap.snapshot_id if latest_snap else None,
            "visible_columns": visible_columns,
            "consumer": consumer,
            "published_at": time.time(),
        }

    def refresh_view(self, lake_table: str, view_snapshot: str,
                     consumer: str | None = None) -> dict[str, Any]:
        """触发视图刷新：按确认后的清单解析位置并记录读取版本。"""
        available = self._available_snapshots(lake_table)
        if not available:
            return {"success": False, "error": "no available snapshot"}
        latest = available[-1]
        manifest = self._confirmed_manifest(lake_table)
        # 冷层消费上线前必须先跑完迁移流水线：有迁移记录的表，
        # 消费侧必须按已确认的清单版本读取，不能直接摸原始物理文件。
        if manifest is None and lake_store.list_tier_migrations(lake_table):
            return {
                "success": False,
                "error": "migration manifest not confirmed",
                "refreshed_snapshot": latest.snapshot_id,
            }
        locations = lake_store.resolve_snapshot_locations(lake_table, latest, manifest)
        if not locations:
            return {
                "success": False,
                "error": "no readable location for snapshot",
                "refreshed_snapshot": latest.snapshot_id,
            }

        total_rows = sum(loc["rows"] for loc in locations)
        location_versions = [
            {
                "logical_file_id": loc["logical_file_id"],
                "generation": loc["generation"],
                "file_id": loc["file_id"],
                "locator": loc["locator"],
                "rows": loc["rows"],
                "bytes": loc["bytes"],
                "tier": loc["tier"],
                "manifest_version": manifest.version_id if manifest else None,
            }
            for loc in locations
        ]
        read = ViewReadRecord(
            read_id=f"vr-{lake_table}-{uuid.uuid4().hex[:8]}",
            lake_table=lake_table,
            view_snapshot=view_snapshot,
            read_snapshot=latest.snapshot_id,
            read_manifest_version=manifest.version_id if manifest else None,
            location_versions=location_versions,
            total_rows=total_rows,
            created_at=time.time(),
        )
        lake_store.record_view_read(read)

        migrations = (
            lake_store.migrations_for_manifest(lake_table, manifest.version_id)
            if manifest is not None
            else []
        )
        pending_receipts = [
            m.migration_id for m in migrations
            if m.receipt_state == "pending" and m.state != "canceled"
        ]
        result = {
            "success": True,
            "previous_snapshot": view_snapshot,
            "refreshed_snapshot": latest.snapshot_id,
            "read_id": read.read_id,
            "manifest_version": manifest.version_id if manifest else None,
            "read_location_versions": len(location_versions),
            "total_rows": total_rows,
            "pending_receipts": pending_receipts,
            "consumer": consumer,
            "refresh_time": read.created_at,
        }
        return result

    def reconcile_receipt(self, lake_table: str, migration_id: str,
                          read_id: str | None = None) -> dict[str, Any]:
        """迁移回执补到后归账：回接到刷新实际读取的位置版本。"""
        migration = lake_store.get_tier_migration(migration_id)
        if migration is None:
            return {"success": False, "error": "migration not found"}
        if migration.lake_table != lake_table:
            return {"success": False, "error": "migration belongs to another lake table"}

        read = (
            lake_store.get_view_read(read_id)
            if read_id
            else lake_store.latest_view_read_for_manifest(lake_table, migration.manifest_version)
        )
        if read is None:
            return {
                "success": False,
                "error": "no view read recorded for this manifest version",
                "migration_id": migration_id,
                "manifest_version": migration.manifest_version,
            }
        if read.read_manifest_version != migration.manifest_version:
            return {
                "success": False,
                "error": "view read bound to a different manifest version",
                "read_manifest_version": read.read_manifest_version,
                "migration_manifest_version": migration.manifest_version,
            }

        matched = [
            loc for loc in read.location_versions
            if loc["logical_file_id"] == migration.logical_file_id
        ]
        migration.receipt_state = "received"
        migration.receipt_received_at = time.time()
        if migration.state != "canceled":
            migration.state = "confirmed"
            migration.confirmed_at = migration.receipt_received_at
        migration.reconciled_read_id = read.read_id
        if migration_id not in read.reconciled_migration_ids:
            read.reconciled_migration_ids.append(migration_id)
        return {
            "success": True,
            "migration_id": migration_id,
            "read_id": read.read_id,
            "read_snapshot": read.read_snapshot,
            "manifest_version": read.read_manifest_version,
            "location_matched": bool(matched),
            "location": matched[0] if matched else None,
            "receipt_state": migration.receipt_state,
            "migration_state": migration.state,
        }

    def list_reads(self, lake_table: str) -> list[dict[str, Any]]:
        """列出消费视图读取记录，便于核对刷新读取的位置版本。"""
        return [
            {
                "read_id": r.read_id,
                "lake_table": r.lake_table,
                "view_snapshot": r.view_snapshot,
                "read_snapshot": r.read_snapshot,
                "read_manifest_version": r.read_manifest_version,
                "total_rows": r.total_rows,
                "location_versions": r.location_versions,
                "reconciled_migration_ids": r.reconciled_migration_ids,
                "created_at": r.created_at,
            }
            for r in lake_store.list_view_reads(lake_table)
        ]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "触发刷新":
            lake_table = str(entry.get("所属湖表", ""))
            if lake_table:
                refresh = self.refresh_view(
                    lake_table,
                    str(entry.get("绑定快照", "")),
                    str(entry.get("下游消费方", "")) or None,
                )
                if refresh.get("success"):
                    entry["绑定快照"] = refresh.get("refreshed_snapshot") or entry.get("绑定快照")
                    entry["最近刷新"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                    entry["读取位置版本"] = refresh.get("manifest_version")
                    entry["读取记录"] = refresh.get("read_id")
                    super().run_action(entry_id, action)
                    return entry, (
                        f"消费视图已{action}，绑定快照 {entry['绑定快照']}，"
                        f"位置版本 {refresh.get('manifest_version') or '—'}"
                    )
        return super().run_action(entry_id, action)

    def revoke_view(self, lake_table: str, consumer: str) -> dict[str, Any]:
        """撤回视图：撤销下游消费方对视图的访问授权。"""
        return {
            "lake_table": lake_table,
            "consumer": consumer,
            "revoked": True,
            "revoke_time": time.time(),
        }
