"""存储分层领域服务：热层/冷层判定、冷层迁移流水线与存量清单版本管理。

冷层迁移不再直接改写物理文件的字节数，而是：

1. 保留热层原始代文件（generation 0），仅登记其逻辑文件身份与定位凭证；
2. 为冷层写入新的代际文件（generation 1），携带独立定位凭证；
3. 把「逻辑文件 -> 代际位置」的映射发布成一版存量清单（manifest version）；
4. 消费视图按已确认的清单版本读取，刷新时记录实际读取的位置版本；
5. 迁移动机具备幂等键，同一份迁移输入重复执行只归账一次节省字节；
6. 取消迁移时把清单回退到上一版，仍可按旧清单读取原快照与文件，
   迁移记录保留可查回的版本痕迹。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import (
    PartitionFile,
    TierManifestEntry,
    TierMigration,
    lake_store,
)

# 冷层压缩后的体积比例：100 字节 -> 40 字节
COLD_BYTES_RATIO = 0.4


def _input_signature(
    lake_table: str,
    compression: str,
    targets: list[tuple[str, str, int]],
    cold_bytes_ratio: float,
) -> str:
    """把一次迁移输入归一成稳定签名，用于幂等重放。"""
    payload = "|".join(
        [
            lake_table,
            compression,
            f"{cold_bytes_ratio:.6f}",
            ",".join(f"{lid}@{fid}#{gen}" for lid, fid, gen in sorted(targets)),
        ]
    )
    return hashlib.md5(payload.encode()).hexdigest()


class StorageTierService(BaseModuleService):
    MODULE = "storage_tier"
    ENTITY = "分层规则"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["分层编号", "所属湖表", "热层判定", "冷层判定", "迁移窗口", "压缩算法", "迁移字节数", "最近迁移"]
    REQUIRED_FIELDS = ["分层编号", "所属湖表", "热层判定"]
    STATUS_ORDER = ["草稿", "已生效", "迁移中", "已暂停"]
    ACTION_RULES = {"启效分层": "已生效", "启动迁移": "迁移中", "暂停分层": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 分层判定
    # ------------------------------------------------------------------

    def classify_to_tier(self, lake_table: str, hot_days: int = 7) -> dict[str, Any]:
        """按时间窗口区分热层/冷层：创建时间超过 hot_days 的分区为冷层。

        已迁移到冷层代际的逻辑文件，无论创建时间如何都按冷层计。
        """
        lake_store.ensure_table_identity(lake_table)
        files = lake_store.list_files(lake_table)
        now = time.time()
        # 每个逻辑文件只看最新代的物理文件，避免迁移前后重复计数
        latest: dict[str, PartitionFile] = {}
        for f in files:
            current = latest.get(f.logical_file_id)
            if current is None or f.generation > current.generation:
                latest[f.logical_file_id] = f
        hot_file_ids: list[str] = []
        cold_file_ids: list[str] = []
        for f in latest.values():
            age_days = (now - f.created_at) / 86400
            if f.tier == "cold" or age_days > hot_days:
                cold_file_ids.append(f.file_id)
            else:
                hot_file_ids.append(f.file_id)
        hot_files = [latest_f for latest_f in latest.values() if latest_f.file_id in hot_file_ids]
        cold_files = [latest_f for latest_f in latest.values() if latest_f.file_id in cold_file_ids]
        return {
            "lake_table": lake_table,
            "hot_file_count": len(hot_file_ids),
            "cold_file_count": len(cold_file_ids),
            "hot_bytes": sum(f.file_bytes for f in hot_files),
            "cold_bytes": sum(f.file_bytes for f in cold_files),
        }

    # ------------------------------------------------------------------
    # 迁移流水线
    # ------------------------------------------------------------------

    def migrate_cold_files(
        self,
        lake_table: str,
        compression: str = "SNAPPY",
        *,
        file_ids: list[str] | None = None,
        hot_days: int | None = None,
        cold_bytes_ratio: float = COLD_BYTES_RATIO,
        idempotency_key: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        """冷层迁移：为选中的热层文件写入冷层新代际，并发布一版存量清单。

        幂等：同一份迁移输入（表、压缩算法、源文件与代际集合）重复执行时，
        直接返回已存在的迁移记录，不重复登记代际、不重复归账节省字节。
        """
        lake_store.ensure_table_identity(lake_table)
        files = lake_store.list_files(lake_table)
        by_id = {f.file_id: f for f in files}

        if file_ids is not None:
            # 只接受属于本湖表的文件；另一张湖表的同名/同 id 文件不会被一起迁移
            candidates = [by_id[fid] for fid in file_ids if fid in by_id]
            ignored = [fid for fid in file_ids if fid not in by_id]
        else:
            candidates = list(files)
            ignored = []

        if hot_days is not None:
            now = time.time()
            candidates = [
                f for f in candidates
                if (now - f.created_at) / 86400 > hot_days
            ]

        # 只有原始热层代（generation 0）参与迁移，已冷层化的代际跳过
        candidates = [
            f for f in candidates
            if f.generation == 0 and f.tier == "hot"
        ]

        # 先按迁移输入（源文件集合）算签名并查幂等记录：
        # 同一份输入无论重复调用多少次，都复用第一次的迁移结果。
        input_targets = [
            (f.logical_file_id, f.file_id, f.generation)
            for f in sorted(candidates, key=lambda item: item.logical_file_id)
        ]
        signature = idempotency_key or _input_signature(
            lake_table, compression, input_targets, cold_bytes_ratio
        )

        existing = lake_store.find_migration_by_key(lake_table, signature)
        if existing is not None and existing.state != "canceled":
            return self._migration_summary(existing, replayed=True)

        # 已有有效（未取消）迁移的逻辑文件不再重复迁移，避免节省字节重复归账
        already_migrated = {
            m.logical_file_id
            for m in lake_store.list_tier_migrations(lake_table)
            if m.state != "canceled"
        }
        candidates = [
            f for f in candidates if f.logical_file_id not in already_migrated
        ]
        # 同一逻辑文件在一次迁移里只迁一次
        selected: dict[str, PartitionFile] = {}
        for f in candidates:
            selected.setdefault(f.logical_file_id, f)

        targets = [
            (f.logical_file_id, f.file_id, f.generation)
            for f in sorted(selected.values(), key=lambda item: item.logical_file_id)
        ]

        if not targets:
            active = lake_store.get_active_manifest(lake_table)
            return {
                "lake_table": lake_table,
                "compression": compression,
                "migration_id": None,
                "migrations": [],
                "migrated_file_count": 0,
                "target_generation": None,
                "bytes_saved": 0,
                "accounted_savings": self.accounted_savings(lake_table),
                "manifest_version": active.version_id if active else None,
                "replayed": False,
                "ignored_file_ids": ignored,
                "message": "没有可迁移的热层文件",
            }

        target_generation = max(
            max((f.generation for f in files if f.logical_file_id == lid), default=0)
            for lid in selected
        ) + 1

        # 冷层消费上线前先确保有一版热层存量清单，取消迁移时才有“旧版”可还原
        baseline = lake_store.get_active_manifest(lake_table)
        if baseline is None:
            baseline = lake_store.publish_manifest(
                lake_table,
                lake_store.build_manifest_entries(lake_table),
                note="迁移前热层存量清单基线",
            )

        migrations: list[TierMigration] = []
        for logical_id in sorted(selected):
            source = selected[logical_id]
            target_bytes = max(1, int(round(source.file_bytes * cold_bytes_ratio)))
            savings = source.file_bytes - target_bytes
            target_file_id = f"{lake_table}-cold-{uuid.uuid4().hex[:8]}"
            # 定位凭证带物理文件标识，保证不同代际/不同轮次的冷层副本互不覆盖
            cold_name = f"{target_file_id}.parquet"
            cold_locator = (
                f"/lake/{lake_table}/{source.partition_key}/gen{target_generation}/{cold_name}"
            )
            cold_file = lake_store.register_file(
                lake_table,
                source.partition_key,
                rows=source.file_rows,
                bytes_=target_bytes,
                logical_file_id=source.logical_file_id,
                generation=target_generation,
                tier="cold",
                file_name=cold_name,
                locator=cold_locator,
                file_id=target_file_id,
            )
            migration = TierMigration(
                migration_id=f"tier-mig-{lake_table}-{uuid.uuid4().hex[:8]}",
                lake_table=lake_table,
                logical_file_id=logical_id,
                source_file_id=source.file_id,
                source_generation=source.generation,
                target_generation=target_generation,
                target_file_id=cold_file.file_id,
                source_bytes=source.file_bytes,
                target_bytes=target_bytes,
                rows=source.file_rows,
                compression=compression,
                state="receipt_pending",
                receipt_state="pending",
                savings_bytes=savings,
                savings_accounted=False,
                manifest_version="",
                idempotency_key=signature,
                snapshot_id=None,
                created_at=time.time(),
            )
            migrations.append(migration)

        # 发布存量清单：只替换本次迁移的逻辑文件，其余逻辑文件沿用现有位置
        active = lake_store.get_active_manifest(lake_table)
        generation_map = {m.logical_file_id: m.target_generation for m in migrations}
        migrated_entries = lake_store.build_manifest_entries(
            lake_table,
            generation_by_logical=generation_map,
            only_logical_ids=set(generation_map),
        )
        carry_over: list[TierManifestEntry] = []
        if active is not None:
            migrated_ids = set(generation_map)
            carry_over = [
                e for e in active.entries if e.logical_file_id not in migrated_ids
            ]
        entries = sorted(carry_over + migrated_entries, key=lambda e: e.logical_file_id)
        version = lake_store.publish_manifest(
            lake_table,
            entries,
            migration_ids=[m.migration_id for m in migrations],
            note=note or f"{compression} 冷层迁移，目标代际 {target_generation}",
        )

        for migration in migrations:
            migration.manifest_version = version.version_id
            # 清单发布即登记为一次有效迁移；同一输入重放复用本记录，节省只归账一次
            migration.savings_accounted = True
            migration.snapshot_id = self._latest_available_snapshot(lake_table)
            lake_store.register_tier_migration(migration)

        result = self._migration_summary(migrations[0], replayed=False)
        result.update(
            {
                "migrations": [self._migration_dict(m) for m in migrations],
                "migrated_file_count": len(migrations),
                "target_generation": target_generation,
                "manifest_version": version.version_id,
                "ignored_file_ids": ignored,
            }
        )
        return result

    def run_migration_pipeline(
        self,
        lake_table: str,
        compression: str = "SNAPPY",
        *,
        file_ids: list[str] | None = None,
        hot_days: int | None = None,
        cold_bytes_ratio: float = COLD_BYTES_RATIO,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """冷层消费上线前的迁移流水线：判定 -> 迁移 -> 发布清单 -> 读取校验。

        校验要求：每个可读快照都能按已发布清单解析到同一逻辑文件的完整位置，
        且行数与迁移前一致（示例文件为 5 行）。
        """
        classification = self.classify_to_tier(
            lake_table, hot_days=hot_days if hot_days is not None else 7
        )
        migration = self.migrate_cold_files(
            lake_table,
            compression,
            file_ids=file_ids,
            hot_days=hot_days,
            cold_bytes_ratio=cold_bytes_ratio,
            idempotency_key=idempotency_key,
            note="冷层消费上线前迁移流水线",
        )
        manifest_version = migration.get("manifest_version")
        verification = self.verify_readable_snapshots(
            lake_table, manifest_version=manifest_version
        )
        return {
            "lake_table": lake_table,
            "classification": classification,
            "migration": migration,
            "manifest_version": manifest_version,
            "verification": verification,
            "ready_for_cold_consumption": bool(
                verification["all_snapshots_readable"] and verification["rows_consistent"]
            ),
        }

    # ------------------------------------------------------------------
    # 读取校验 / 清单回查
    # ------------------------------------------------------------------

    def _latest_available_snapshot(self, lake_table: str) -> str | None:
        available = [
            s for s in lake_store.list_snapshots(lake_table)
            if s.status == "available"
        ]
        return available[-1].snapshot_id if available else None

    def verify_readable_snapshots(
        self,
        lake_table: str,
        manifest_version: str | None = None,
    ) -> dict[str, Any]:
        """逐个校验可读快照：能否定位到完整位置、行数是否与原始一致。"""
        manifest = (
            lake_store.get_manifest_version(lake_table, manifest_version)
            if manifest_version
            else lake_store.get_active_manifest(lake_table)
        )
        snapshots = [
            s for s in lake_store.list_snapshots(lake_table)
            if s.status == "available"
        ]
        lake_store.ensure_table_identity(lake_table)
        migrations = lake_store.list_tier_migrations(lake_table)
        details: list[dict[str, Any]] = []
        all_readable = True
        rows_consistent = True
        for snap in snapshots:
            locations = lake_store.resolve_snapshot_locations(lake_table, snap, manifest)
            expected_logicals: set[str] = set()
            original_rows = 0
            for fid in snap.file_ids:
                source = lake_store.get_file(lake_table, fid)
                if source is None:
                    continue
                logical_id = source.logical_file_id
                expected_logicals.add(logical_id)
                migration = next(
                    (
                        m for m in migrations
                        if m.logical_file_id == logical_id and m.source_file_id == fid
                    ),
                    None,
                )
                original_rows += migration.rows if migration else source.file_rows
            found = {loc["logical_file_id"] for loc in locations}
            complete = expected_logicals.issubset(found) and all(
                loc.get("locator") for loc in locations
            )
            rows = sum(loc["rows"] for loc in locations)
            row_ok = rows == original_rows
            all_readable = all_readable and complete
            rows_consistent = rows_consistent and row_ok
            details.append({
                "snapshot_id": snap.snapshot_id,
                "status": snap.status,
                "manifest_version": manifest.version_id if manifest else None,
                "file_count": len(locations),
                "total_rows": rows,
                "original_rows": original_rows,
                "rows_consistent": row_ok,
                "locations": locations,
            })
        return {
            "lake_table": lake_table,
            "manifest_version": manifest.version_id if manifest else None,
            "snapshot_count": len(details),
            "all_snapshots_readable": all_readable,
            "rows_consistent": rows_consistent,
            "snapshots": details,
        }

    def snapshot_locations(
        self, lake_table: str, snapshot_id: str, manifest_version: str | None = None
    ) -> dict[str, Any]:
        """按快照 + 清单版本解析逻辑文件位置，供消费视图刷新使用。"""
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "error": "snapshot not found", "locations": []}
        manifest = (
            lake_store.get_manifest_version(lake_table, manifest_version)
            if manifest_version
            else lake_store.get_active_manifest(lake_table)
        )
        locations = lake_store.resolve_snapshot_locations(lake_table, snap, manifest)
        return {
            "success": True,
            "snapshot_id": snap.snapshot_id,
            "manifest_version": manifest.version_id if manifest else None,
            "locations": locations,
            "total_rows": sum(loc["rows"] for loc in locations),
        }

    # ------------------------------------------------------------------
    # 迁移记录、回执与取消
    # ------------------------------------------------------------------

    def _migration_dict(self, migration: TierMigration) -> dict[str, Any]:
        source = lake_store.get_file(migration.lake_table, migration.source_file_id)
        target = lake_store.get_file(migration.lake_table, migration.target_file_id)
        return {
            "migration_id": migration.migration_id,
            "lake_table": migration.lake_table,
            "logical_file_id": migration.logical_file_id,
            "source_file_id": migration.source_file_id,
            "source_generation": migration.source_generation,
            "target_generation": migration.target_generation,
            "target_file_id": migration.target_file_id,
            "source_bytes": migration.source_bytes,
            "target_bytes": migration.target_bytes,
            "rows": migration.rows,
            "compression": migration.compression,
            "state": migration.state,
            "receipt_state": migration.receipt_state,
            "savings_bytes": migration.savings_bytes,
            "savings_accounted": migration.savings_accounted,
            "manifest_version": migration.manifest_version,
            "snapshot_id": migration.snapshot_id,
            "created_at": migration.created_at,
            "confirmed_at": migration.confirmed_at,
            "canceled_at": migration.canceled_at,
            "receipt_received_at": migration.receipt_received_at,
            "reconciled_read_id": migration.reconciled_read_id,
            "source_locator": source.locator if source else None,
            "target_locator": target.locator if target else None,
            "target_file_present": target is not None,
        }

    def _migration_summary(self, migration: TierMigration, *, replayed: bool) -> dict[str, Any]:
        related = lake_store.migrations_for_manifest(
            migration.lake_table, migration.manifest_version
        )
        return {
            "lake_table": migration.lake_table,
            "compression": migration.compression,
            "migration_id": migration.migration_id,
            "migrations": [self._migration_dict(m) for m in (related or [migration])],
            # 重放时复用的是原迁移，文件数照实报告，但不再新增归账
            "migrated_file_count": len(related) if related else 1,
            "target_generation": migration.target_generation,
            "bytes_saved": 0 if replayed else migration.savings_bytes,
            "accounted_savings": self.accounted_savings(migration.lake_table),
            "manifest_version": migration.manifest_version,
            "replayed": replayed,
            "ignored_file_ids": [],
            "message": "同一份迁移输入已处理，复用原迁移记录" if replayed else "冷层迁移完成",
        }

    def accounted_savings(self, lake_table: str) -> int:
        """已归账的节省字节：只统计一次有效迁移，取消迁移不计入。"""
        return sum(
            m.savings_bytes
            for m in lake_store.list_tier_migrations(lake_table)
            if m.savings_accounted and m.state != "canceled"
        )

    def get_migration(self, migration_id: str) -> dict[str, Any] | None:
        migration = lake_store.get_tier_migration(migration_id)
        return self._migration_dict(migration) if migration else None

    def list_migrations(self, lake_table: str | None = None) -> list[dict[str, Any]]:
        return [self._migration_dict(m) for m in lake_store.list_tier_migrations(lake_table)]

    def record_receipt(self, migration_id: str) -> dict[str, Any]:
        """迁移回执补到：确认迁移并回接到实际读取的位置版本上归账。"""
        migration = lake_store.get_tier_migration(migration_id)
        if migration is None:
            return {"success": False, "error": "migration not found"}
        migration.receipt_state = "received"
        migration.receipt_received_at = time.time()
        if migration.state == "canceled":
            return {
                "success": False,
                "error": "migration canceled",
                "migration": self._migration_dict(migration),
            }
        migration.state = "confirmed"
        migration.confirmed_at = migration.receipt_received_at
        migration.savings_accounted = True
        view_read = lake_store.latest_view_read_for_manifest(
            migration.lake_table, migration.manifest_version
        )
        if view_read is not None:
            migration.reconciled_read_id = view_read.read_id
            if migration.migration_id not in view_read.reconciled_migration_ids:
                view_read.reconciled_migration_ids.append(migration.migration_id)
        return {
            "success": True,
            "migration": self._migration_dict(migration),
            "reconciled_read_id": migration.reconciled_read_id,
            "read_manifest_version": view_read.read_manifest_version if view_read else None,
            "location_versions": view_read.location_versions if view_read else [],
            "accounted_savings": self.accounted_savings(migration.lake_table),
        }

    def cancel_migration(self, migration_id: str) -> dict[str, Any]:
        """取消本轮迁移：回退清单到上一版，冷层文件撤下，旧清单仍可读取。"""
        migration = lake_store.get_tier_migration(migration_id)
        if migration is None:
            return {"success": False, "error": "migration not found"}
        if migration.state == "canceled":
            return {"success": True, "migration": self._migration_dict(migration)}

        manifest = lake_store.get_manifest_version(
            migration.lake_table, migration.manifest_version
        )
        version_ids = [
            v.version_id for v in lake_store.list_manifest_versions(migration.lake_table)
        ]
        is_active = (
            manifest is not None
            and lake_store.get_active_manifest(migration.lake_table) is manifest
        )
        if not is_active:
            return {
                "success": False,
                "error": "only the active manifest migration can be canceled",
                "migration": self._migration_dict(migration),
            }

        # 本轮涉及的同清单迁移一起取消，清单回退一次
        sibling_ids = manifest.migration_ids if manifest is not None else [migration_id]
        siblings = [
            m for m in (lake_store.get_tier_migration(mid) for mid in sibling_ids)
            if m is not None and m.state != "canceled"
        ]

        restored_version_id = None
        if is_active:
            previous = (
                lake_store.get_manifest_version(
                    migration.lake_table, manifest.previous_version_id
                )
                if manifest.previous_version_id
                else None
            )
            manifest.status = "rolled_back"
            if previous is not None:
                previous.status = "active"
                restored_version_id = previous.version_id
            # 冷层目标文件撤下；热层原始代保留，清单缺省回落到物理文件即可读
            removed = [
                sibling.target_file_id
                for sibling in siblings
                if lake_store.get_file(sibling.lake_table, sibling.target_file_id) is not None
            ]
            lake_store.remove_files(migration.lake_table, removed)

        for sibling in siblings:
            sibling.state = "canceled"
            sibling.savings_accounted = False
            sibling.canceled_at = time.time()

        migration.state = "canceled"
        migration.savings_accounted = False
        migration.canceled_at = time.time()

        verification = self.verify_readable_snapshots(
            migration.lake_table, manifest_version=restored_version_id
        )
        return {
            "success": True,
            "migration": self._migration_dict(migration),
            "restored_manifest_version": restored_version_id,
            "rolled_back_manifest_version": manifest.version_id if manifest else None,
            "manifest_history": version_ids,
            "accounted_savings": self.accounted_savings(migration.lake_table),
            "verification": verification,
        }

    # ------------------------------------------------------------------
    # 存储分层详情页
    # ------------------------------------------------------------------

    def tier_detail(self, lake_table: str) -> dict[str, Any]:
        """存储分层详情：列出每个逻辑文件的代际位置，重点核对冷层定位凭证。"""
        lake_store.ensure_table_identity(lake_table)
        active = lake_store.get_active_manifest(lake_table)
        files = sorted(
            lake_store.list_files(lake_table),
            key=lambda f: (f.logical_file_id, f.generation),
        )
        file_rows = [
            {
                "logical_file_id": f.logical_file_id,
                "generation": f.generation,
                "tier": f.tier,
                "file_id": f.file_id,
                "file_name": f.file_name,
                "locator": f.locator,
                "rows": f.file_rows,
                "bytes": f.file_bytes,
                "is_active_location": bool(
                    active is not None
                    and any(
                        e.logical_file_id == f.logical_file_id
                        and e.generation == f.generation
                        for e in active.entries
                    )
                ),
            }
            for f in files
        ]
        return {
            "lake_table": lake_table,
            "classification": self.classify_to_tier(lake_table),
            "active_manifest_version": active.version_id if active else None,
            "manifest_versions": [
                {
                    "version_id": v.version_id,
                    "version_number": v.version_number,
                    "status": v.status,
                    "entry_count": len(v.entries),
                    "migration_ids": v.migration_ids,
                    "previous_version_id": v.previous_version_id,
                    "created_at": v.created_at,
                    "entries": [
                        {
                            "logical_file_id": e.logical_file_id,
                            "generation": e.generation,
                            "locator": e.locator,
                            "rows": e.rows,
                            "bytes": e.bytes,
                            "tier": e.tier,
                            "file_id": e.file_id,
                        }
                        for e in v.entries
                    ],
                }
                for v in lake_store.list_manifest_versions(lake_table)
            ],
            "files": file_rows,
            "migrations": self.list_migrations(lake_table),
            "accounted_savings": self.accounted_savings(lake_table),
        }

    def entry_detail(self, entry_id: int) -> dict[str, Any] | None:
        """给详情页补充分层规则对应湖表的清单与冷层定位凭证。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        lake_table = str(entry.get("所属湖表", ""))
        detail = dict(entry)
        detail["detail"] = self.tier_detail(lake_table) if lake_table else None
        return detail

    # ------------------------------------------------------------------
    # 规则流转：启动迁移时真正跑迁移流水线
    # ------------------------------------------------------------------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "启动迁移":
            lake_table = str(entry.get("所属湖表", ""))
            if lake_table:
                pipeline = self.run_migration_pipeline(
                    lake_table,
                    compression=str(entry.get("压缩算法") or "SNAPPY"),
                )
                entry["迁移字节数"] = self.accounted_savings(lake_table)
                entry["最近迁移"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                entry["清单版本"] = pipeline.get("manifest_version")
                super().run_action(entry_id, action)
                return entry, (
                    f"分层规则已{action}，清单版本 {pipeline.get('manifest_version')}，"
                    f"迁移 {pipeline['migration'].get('migrated_file_count', 0)} 个文件"
                )
        return super().run_action(entry_id, action)
