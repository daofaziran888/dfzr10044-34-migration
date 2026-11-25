"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。

    除物理文件标识外，这里同时承载“逻辑文件身份”与“代际位置”：
    - logical_file_id: 同一个逻辑文件在冷热分层前后保持稳定的身份
    - generation: 代际位置，0 为热层原始代，1+ 为冷层迁移后产生的代
    - tier: hot | cold
    - locator: 定位凭证，读取方据此定位到该代的物理位置
    """
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    logical_file_id: str = ""
    generation: int = 0
    tier: str = "hot"
    locator: str = ""
    file_name: str = ""


# ----------------------------- 存量清单 -----------------------------

@dataclass
class TierManifestEntry:
    """存量清单条目：某逻辑文件在某一代际上的完整位置。"""
    logical_file_id: str
    generation: int
    locator: str
    rows: int
    bytes: int
    tier: str
    file_id: str


@dataclass
class TierManifestVersion:
    """存量清单版本：一次提交后的完整逻辑文件位置清单，可回查、可回退。"""
    lake_table: str
    version_id: str
    version_number: int
    entries: list["TierManifestEntry"]
    status: str  # active | superseded | rolled_back
    created_at: float
    migration_ids: list[str] = field(default_factory=list)
    note: str = ""
    previous_version_id: str | None = None


@dataclass
class TierMigration:
    """冷层迁移记录：热层某代文件迁移到冷层的可核对版本记录。"""
    migration_id: str
    lake_table: str
    logical_file_id: str
    source_file_id: str
    source_generation: int
    target_generation: int
    target_file_id: str
    source_bytes: int
    target_bytes: int
    rows: int
    compression: str
    state: str  # writing | receipt_pending | confirmed | canceled
    receipt_state: str  # pending | received
    savings_bytes: int
    savings_accounted: bool
    manifest_version: str
    idempotency_key: str
    snapshot_id: str | None
    created_at: float
    confirmed_at: float | None = None
    canceled_at: float | None = None
    receipt_received_at: float | None = None
    reconciled_read_id: str | None = None


@dataclass
class ViewReadRecord:
    """消费视图读取记录：绑定本次刷新实际读取到的位置版本，供迟到回执归账。"""
    read_id: str
    lake_table: str
    view_snapshot: str | None
    read_snapshot: str | None
    read_manifest_version: str | None
    location_versions: list[dict[str, Any]]
    total_rows: int
    created_at: float
    reconciled_migration_ids: list[str] = field(default_factory=list)


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 存量清单版本：lake_table -> list[TierManifestVersion]（时间序）
        self._tier_manifests: dict[str, list[TierManifestVersion]] = {}

        # 冷层迁移记录：migration_id -> TierMigration
        self._tier_migrations: dict[str, TierMigration] = {}

        # 幂等键索引：(lake_table, idempotency_key) -> migration_id
        self._tier_migration_keys: dict[tuple[str, str], str] = {}

        # 消费视图读取记录：read_id -> ViewReadRecord
        self._view_reads: dict[str, ViewReadRecord] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None,
        *,
        logical_file_id: str | None = None,
        generation: int = 0,
        tier: str = "hot",
        file_name: str | None = None,
        locator: str | None = None,
        file_id: str | None = None,
    ) -> PartitionFile:
        file_id = file_id or f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        name = file_name or f"{file_id}.parquet"
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            logical_file_id=logical_file_id or file_id,
            generation=generation,
            tier=tier,
            locator=locator or f"/lake/{lake_table}/{partition_key}/{name}",
            file_name=name,
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 存量清单与冷层迁移 ----

    def _ensure_file_identity(self, f: PartitionFile) -> None:
        """补齐逻辑文件身份、代际位置与定位凭证，兼容早期只登记物理文件的记录。"""
        if not f.logical_file_id:
            f.logical_file_id = f.file_id
        if not f.locator:
            f.locator = f"/lake/{f.lake_table}/{f.partition_key}/{f.file_id}.parquet"
        if not f.file_name:
            f.file_name = f"{f.file_id}.parquet"

    def ensure_table_identity(self, lake_table: str) -> None:
        for f in self.list_files(lake_table):
            self._ensure_file_identity(f)

    def get_active_manifest(self, lake_table: str) -> TierManifestVersion | None:
        active = [v for v in self._tier_manifests.get(lake_table, []) if v.status == "active"]
        return active[-1] if active else None

    def get_manifest_version(self, lake_table: str, version_id: str) -> TierManifestVersion | None:
        for version in self._tier_manifests.get(lake_table, []):
            if version.version_id == version_id:
                return version
        return None

    def list_manifest_versions(self, lake_table: str) -> list[TierManifestVersion]:
        return list(self._tier_manifests.get(lake_table, []))

    def publish_manifest(
        self,
        lake_table: str,
        entries: list[TierManifestEntry],
        migration_ids: list[str] | None = None,
        note: str = "",
    ) -> TierManifestVersion:
        """发布一版存量清单：上一版转为 superseded 但仍可回查。"""
        versions = self._tier_manifests.setdefault(lake_table, [])
        previous = None
        for version in versions:
            if version.status == "active":
                version.status = "superseded"
                previous = version.version_id
        number = max((v.version_number for v in versions), default=0) + 1
        version = TierManifestVersion(
            lake_table=lake_table,
            version_id=f"tmn-{lake_table}-v{number:04d}",
            version_number=number,
            entries=[copy.copy(e) for e in entries],
            status="active",
            created_at=time.time(),
            migration_ids=list(migration_ids or []),
            note=note,
            previous_version_id=previous,
        )
        versions.append(version)
        return version

    def build_manifest_entries(
        self,
        lake_table: str,
        generation_by_logical: dict[str, int] | None = None,
        only_logical_ids: set[str] | None = None,
    ) -> list[TierManifestEntry]:
        """按逻辑文件聚合物理文件，为每个逻辑文件选出目标代际的位置。

        generation_by_logical 指定逻辑文件应落在哪一代；未指定时取该逻辑文件的最新代。
        only_logical_ids 限定参与构建的逻辑文件（用于局部迁移），其余逻辑文件沿用当前清单。
        """
        self.ensure_table_identity(lake_table)
        files = self.list_files(lake_table)
        by_logical: dict[str, list[PartitionFile]] = {}
        for f in files:
            by_logical.setdefault(f.logical_file_id, []).append(f)

        entries: list[TierManifestEntry] = []
        for logical_id, gens in by_logical.items():
            if only_logical_ids is not None and logical_id not in only_logical_ids:
                continue
            target_gen = (generation_by_logical or {}).get(logical_id)
            if target_gen is None:
                target_gen = max(f.generation for f in gens)
            candidates = [f for f in gens if f.generation == target_gen]
            if not candidates:
                continue
            chosen = candidates[-1]
            entries.append(TierManifestEntry(
                logical_file_id=logical_id,
                generation=chosen.generation,
                locator=chosen.locator,
                rows=chosen.file_rows,
                bytes=chosen.file_bytes,
                tier=chosen.tier,
                file_id=chosen.file_id,
            ))
        entries.sort(key=lambda e: e.logical_file_id)
        return entries

    def resolve_snapshot_locations(
        self,
        lake_table: str,
        snapshot: LakeSnapshot,
        manifest: TierManifestVersion | None = None,
    ) -> list[dict[str, Any]]:
        """把快照里的物理文件清单解析成同一逻辑文件的完整位置。

        迁移后旧快照仍指向热层文件，这里按逻辑身份重定位到当前清单中的代际位置，
        保证每个可读快照都能找到完整定位凭证与原始行数。
        """
        if manifest is None:
            manifest = self.get_active_manifest(lake_table)
        by_logical = (
            {e.logical_file_id: e for e in manifest.entries} if manifest is not None else {}
        )
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for fid in snapshot.file_ids:
            f = self.get_file(lake_table, fid)
            if f is None:
                continue
            self._ensure_file_identity(f)
            logical_id = f.logical_file_id
            if logical_id in seen:
                continue
            seen.add(logical_id)
            entry = by_logical.get(logical_id)
            if entry is not None:
                resolved.append({
                    "logical_file_id": entry.logical_file_id,
                    "generation": entry.generation,
                    "file_id": entry.file_id,
                    "locator": entry.locator,
                    "rows": entry.rows,
                    "bytes": entry.bytes,
                    "tier": entry.tier,
                    "resolved_from_file_id": fid,
                    "manifest_version": manifest.version_id if manifest else None,
                })
            else:
                resolved.append({
                    "logical_file_id": logical_id,
                    "generation": f.generation,
                    "file_id": f.file_id,
                    "locator": f.locator,
                    "rows": f.file_rows,
                    "bytes": f.file_bytes,
                    "tier": f.tier,
                    "resolved_from_file_id": fid,
                    "manifest_version": None,
                })
        return resolved

    # ---- 冷层迁移记录 ----

    def find_migration_by_key(self, lake_table: str, idempotency_key: str) -> TierMigration | None:
        mid = self._tier_migration_keys.get((lake_table, idempotency_key))
        return self._tier_migrations.get(mid) if mid else None

    def register_tier_migration(self, migration: TierMigration) -> TierMigration:
        self._tier_migrations[migration.migration_id] = migration
        if migration.idempotency_key:
            self._tier_migration_keys[
                (migration.lake_table, migration.idempotency_key)
            ] = migration.migration_id
        return migration

    def get_tier_migration(self, migration_id: str) -> TierMigration | None:
        return self._tier_migrations.get(migration_id)

    def list_tier_migrations(self, lake_table: str | None = None) -> list[TierMigration]:
        values = list(self._tier_migrations.values())
        if lake_table is not None:
            values = [m for m in values if m.lake_table == lake_table]
        return values

    def migrations_for_manifest(self, lake_table: str, version_id: str) -> list[TierMigration]:
        return [
            m for m in self.list_tier_migrations(lake_table)
            if m.manifest_version == version_id
        ]

    # ---- 消费视图读取记录 ----

    def record_view_read(self, record: ViewReadRecord) -> ViewReadRecord:
        self._view_reads[record.read_id] = record
        return record

    def get_view_read(self, read_id: str) -> ViewReadRecord | None:
        return self._view_reads.get(read_id)

    def list_view_reads(self, lake_table: str | None = None) -> list[ViewReadRecord]:
        values = list(self._view_reads.values())
        if lake_table is not None:
            values = [r for r in values if r.lake_table == lake_table]
        return values

    def latest_view_read_for_manifest(self, lake_table: str, version_id: str) -> ViewReadRecord | None:
        matches = [
            r for r in self.list_view_reads(lake_table)
            if r.read_manifest_version == version_id
        ]
        return matches[-1] if matches else None

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()