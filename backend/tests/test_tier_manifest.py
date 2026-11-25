"""冷层迁移与存量清单场景测试：逻辑文件身份、代际位置、迟到回执、取消回退与跨表隔离。"""
from __future__ import annotations

import copy

import pytest

from app.lake_store import LakeStore
from app.services.publish_view import PublishViewService
from app.services.storage_tier import StorageTierService


@pytest.fixture
def scenario(monkeypatch):
    """两个湖表各有一个 100 字节、5 行的同名文件，并各生成一个可用快照。"""
    import app.services.publish_view as pv_mod
    import app.services.storage_tier as st_mod

    lake_store = LakeStore()
    monkeypatch.setattr(st_mod, "lake_store", lake_store)
    monkeypatch.setattr(pv_mod, "lake_store", lake_store)
    storage = StorageTierService()
    view = PublishViewService()

    hot_a = lake_store.register_file(
        "lake_a", "20260101", rows=5, bytes_=100,
        file_name="part-000.parquet",
        file_id="lake_a-part-000",
    )
    hot_b = lake_store.register_file(
        "lake_b", "20260101", rows=5, bytes_=100,
        file_name="part-000.parquet",
        file_id="lake_b-part-000",
    )
    snap_a = lake_store.create_snapshot("lake_a", [hot_a.file_id], added_rows=5)
    lake_store.mark_snapshot_available("lake_a", snap_a.snapshot_id)
    snap_b = lake_store.create_snapshot("lake_b", [hot_b.file_id], added_rows=5)
    lake_store.mark_snapshot_available("lake_b", snap_b.snapshot_id)
    yield {
        "store": lake_store,
        "storage": storage,
        "view": view,
        "hot_a": hot_a,
        "hot_b": hot_b,
        "snap_a": snap_a.snapshot_id,
        "snap_b": snap_b.snapshot_id,
    }


def _savings(store: LakeStore, table: str) -> int:
    return sum(
        m.savings_bytes
        for m in store.list_tier_migrations(table)
        if m.savings_accounted and m.state != "canceled"
    )


class TestColdMigrationScenario:
    def test_migration_reports_40_bytes_and_60_saved(self, scenario):
        storage = scenario["storage"]
        result = storage.migrate_cold_files("lake_a", "SNAPPY")
        assert result["migrated_file_count"] == 1
        mig = result["migrations"][0]
        assert mig["source_bytes"] == 100
        assert mig["target_bytes"] == 40
        assert mig["savings_bytes"] == 60
        assert mig["target_generation"] == 1
        assert mig["state"] == "receipt_pending"
        # 热层原始代不被就地改写体积
        hot = scenario["store"].get_file("lake_a", scenario["hot_a"].file_id)
        assert hot.file_bytes == 100
        # 冷层独立代际带定位凭证
        cold = scenario["store"].get_file("lake_a", mig["target_file_id"])
        assert cold.file_bytes == 40
        assert cold.generation == 1
        assert cold.tier == "cold"
        assert cold.locator and cold.locator != hot.locator

    def test_every_readable_snapshot_finds_same_logical_file_and_rows(self, scenario):
        storage = scenario["storage"]
        pipeline = storage.run_migration_pipeline("lake_a", "SNAPPY")
        verification = pipeline["verification"]
        assert verification["all_snapshots_readable"]
        assert verification["rows_consistent"]
        for snap_detail in verification["snapshots"]:
            assert snap_detail["total_rows"] == 5
            assert len(snap_detail["locations"]) == 1
            loc = snap_detail["locations"][0]
            assert loc["logical_file_id"] == "lake_a-part-000"
            assert loc["generation"] == 1
            assert loc["locator"]
        # 迁移前后快照仍能按逻辑文件身份读到同一完整位置
        snap = scenario["store"].get_snapshot("lake_a", scenario["snap_a"])
        locations = scenario["store"].resolve_snapshot_locations("lake_a", snap)
        assert [loc["logical_file_id"] for loc in locations] == ["lake_a-part-000"]
        assert locations[0]["rows"] == 5

    def test_savings_accounted_only_once_for_same_input(self, scenario):
        storage = scenario["storage"]
        first = storage.migrate_cold_files("lake_a", "SNAPPY")
        second = storage.migrate_cold_files("lake_a", "SNAPPY")
        assert second["replayed"] is True
        assert second["migration_id"] == first["migration_id"]
        assert second["bytes_saved"] == 0
        assert _savings(scenario["store"], "lake_a") == 60
        # 只登记一次冷层代际
        cold_files = [
            f for f in scenario["store"].list_files("lake_a") if f.tier == "cold"
        ]
        assert len(cold_files) == 1

    def test_late_receipt_then_refresh_then_replay(self, scenario):
        store = scenario["store"]
        storage = scenario["storage"]
        view = scenario["view"]

        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        migration_id = migration["migration_id"]
        assert store.get_tier_migration(migration_id).receipt_state == "pending"

        # 回执迟到时先刷新一次消费视图，刷新记录实际读取的位置版本
        refresh = view.refresh_view("lake_a", scenario["snap_a"], "ADHOC_QUERY")
        assert refresh["success"]
        assert refresh["manifest_version"] == migration["manifest_version"]
        assert refresh["read_location_versions"] == 1
        assert refresh["total_rows"] == 5
        read = store.get_view_read(refresh["read_id"])
        assert read is not None
        assert read.location_versions[0]["logical_file_id"] == "lake_a-part-000"
        assert read.location_versions[0]["generation"] == 1

        # 再用同一份迁移输入恢复：幂等，不重复归账
        restored = storage.run_migration_pipeline("lake_a", "SNAPPY")
        assert restored["migration"]["replayed"] is True
        assert restored["ready_for_cold_consumption"] is True
        assert _savings(store, "lake_a") == 60

        # 回执补到后回接到那次实际读取的位置版本上归账
        reconcile = view.reconcile_receipt("lake_a", migration_id)
        assert reconcile["success"]
        assert reconcile["read_id"] == refresh["read_id"]
        assert reconcile["location_matched"]
        assert reconcile["location"]["generation"] == 1
        assert store.get_tier_migration(migration_id).state == "confirmed"
        assert _savings(store, "lake_a") == 60

    def test_other_table_same_named_file_not_migrated(self, scenario):
        store = scenario["store"]
        storage = scenario["storage"]
        result = storage.migrate_cold_files("lake_a", "SNAPPY")
        assert result["migrated_file_count"] == 1
        # lake_b 的文件与快照完全不变
        other = store.get_file("lake_b", scenario["hot_b"].file_id)
        assert other.file_bytes == 100
        assert other.generation == 0
        assert other.tier == "hot"
        assert store.list_tier_migrations("lake_b") == []
        assert store.get_active_manifest("lake_b") is None
        assert _savings(store, "lake_b") == 0
        # 传入其他表的文件 id 被忽略，不会顺带迁移
        ignored = storage.migrate_cold_files(
            "lake_a", "SNAPPY", file_ids=[scenario["hot_b"].file_id]
        )
        assert ignored["migrated_file_count"] == 0
        assert ignored["ignored_file_ids"] == [scenario["hot_b"].file_id]

    def test_cancel_restores_old_manifest_and_keeps_version_history(self, scenario):
        store = scenario["store"]
        storage = scenario["storage"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        migration_id = migration["migration_id"]
        cold_file_id = migration["migrations"][0]["target_file_id"]
        migrated_manifest = migration["manifest_version"]

        canceled = storage.cancel_migration(migration_id)
        assert canceled["success"]
        assert canceled["restored_manifest_version"] is not None
        assert canceled["rolled_back_manifest_version"] == migrated_manifest
        # 冷层目标文件撤下，热层文件仍在
        assert store.get_file("lake_a", cold_file_id) is None
        assert store.get_file("lake_a", scenario["hot_a"].file_id) is not None
        assert _savings(store, "lake_a") == 0

        # 按旧清单仍能读到原快照和文件
        verification = canceled["verification"]
        assert verification["all_snapshots_readable"]
        assert verification["rows_consistent"]
        loc = verification["snapshots"][0]["locations"][0]
        assert loc["generation"] == 0
        assert loc["tier"] == "hot"
        assert loc["rows"] == 5
        assert loc["locator"] == scenario["hot_a"].locator

        # 已迁移版本记录仍可查回
        history = [v.version_id for v in store.list_manifest_versions("lake_a")]
        assert migrated_manifest in history
        versions = {v.version_id: v for v in store.list_manifest_versions("lake_a")}
        assert versions[migrated_manifest].status == "rolled_back"
        assert versions[migrated_manifest].migration_ids == [migration_id]
        record = storage.get_migration(migration_id)
        assert record["state"] == "canceled"
        assert record["manifest_version"] == migrated_manifest

    def test_tier_detail_exposes_cold_locator_credential(self, scenario):
        storage = scenario["storage"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        detail = storage.tier_detail("lake_a")
        assert detail["active_manifest_version"] == migration["manifest_version"]
        cold_rows = [f for f in detail["files"] if f["tier"] == "cold"]
        assert len(cold_rows) == 1
        assert cold_rows[0]["locator"].startswith("/lake/lake_a/")
        assert cold_rows[0]["is_active_location"] is True
        active = [
            v for v in detail["manifest_versions"]
            if v["version_id"] == detail["active_manifest_version"]
        ][0]
        assert active["entries"][0]["locator"] == cold_rows[0]["locator"]
        assert active["entries"][0]["rows"] == 5

    def test_all_snapshots_including_post_migration_are_readable(self, scenario):
        store = scenario["store"]
        storage = scenario["storage"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        # 迁移后再生成一个仍指向热层文件的快照，读取时也应重定位到冷层代际
        later = store.create_snapshot("lake_a", [scenario["hot_a"].file_id], added_rows=5)
        store.mark_snapshot_available("lake_a", later.snapshot_id)
        verification = storage.verify_readable_snapshots(
            "lake_a", migration["manifest_version"]
        )
        assert verification["snapshot_count"] == 2
        assert verification["all_snapshots_readable"]
        assert verification["rows_consistent"]
        for snap_detail in verification["snapshots"]:
            assert snap_detail["total_rows"] == 5
            assert snap_detail["locations"][0]["generation"] == 1

    def test_reconcile_requires_a_recorded_read(self, scenario):
        storage = scenario["storage"]
        view = scenario["view"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        result = view.reconcile_receipt("lake_a", migration["migration_id"])
        assert result["success"] is False
        assert "no view read" in result["error"]

    def test_reconcile_rejects_read_from_other_manifest(self, scenario):
        storage = scenario["storage"]
        view = scenario["view"]
        first = storage.migrate_cold_files("lake_a", "SNAPPY")
        first_read = view.refresh_view("lake_a", scenario["snap_a"])
        # 取消后重新迁移会产生新的清单版本与新的迁移记录
        storage.cancel_migration(first["migration_id"])
        second = storage.migrate_cold_files("lake_a", "SNAPPY")
        # 明确把第一次的读取记录交给第二次迁移，版本不匹配应拒绝
        result = view.reconcile_receipt(
            "lake_a", second["migration_id"], first_read["read_id"]
        )
        assert result["success"] is False
        assert "different manifest version" in result["error"]

    def test_refresh_after_cancel_reads_old_location(self, scenario):
        storage = scenario["storage"]
        view = scenario["view"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        storage.cancel_migration(migration["migration_id"])
        refresh = view.refresh_view("lake_a", scenario["snap_a"])
        assert refresh["success"]
        assert refresh["pending_receipts"] == []
        read = scenario["store"].get_view_read(refresh["read_id"])
        assert read.location_versions[0]["generation"] == 0
        assert read.location_versions[0]["tier"] == "hot"
        assert read.location_versions[0]["locator"] == scenario["hot_a"].locator

    def test_receipt_for_canceled_migration_does_not_confirm(self, scenario):
        storage = scenario["storage"]
        migration = storage.migrate_cold_files("lake_a", "SNAPPY")
        storage.cancel_migration(migration["migration_id"])
        result = storage.record_receipt(migration["migration_id"])
        assert result["success"] is False
        assert result["migration"]["state"] == "canceled"
        assert _savings(scenario["store"], "lake_a") == 0

    def test_explicit_idempotency_key_dedupes(self, scenario):
        storage = scenario["storage"]
        first = storage.migrate_cold_files("lake_a", "SNAPPY", idempotency_key="run-001")
        second = storage.migrate_cold_files("lake_a", "SNAPPY", idempotency_key="run-001")
        assert second["replayed"] is True
        assert second["migration_id"] == first["migration_id"]
        assert _savings(scenario["store"], "lake_a") == 60

    def test_detail_entry_via_service(self, scenario, monkeypatch):
        storage = scenario["storage"]
        storage.migrate_cold_files("lake_a", "SNAPPY")
        # 造一条分层规则条目，验证详情接口把清单与冷层凭证挂上
        import app.services.base as base_mod
        monkeypatch.setattr(base_mod, "store", _FakeStore("lake_a"))
        detail = storage.entry_detail(1)
        assert detail is not None
        assert detail["detail"]["active_manifest_version"] is not None
        assert any(f["tier"] == "cold" for f in detail["detail"]["files"])


class _FakeStore:
    def __init__(self, lake_table: str) -> None:
        self._row = {"id": 1, "所属湖表": lake_table, "status": "迁移中"}

    def find(self, module: str, entry_id: int):
        return self._row if entry_id == 1 else None

    def rows(self, module: str):
        return [self._row]
