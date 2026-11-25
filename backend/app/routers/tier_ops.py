"""存储分层与消费视图的专用接口：迁移流水线、回执归账、清单回查与读取记录。

路径刻意与通用资源路由 `/api/{module}` 区分开，避免 `storage_tier` / `publish_view`
的条目接口与这些运维动作互相抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.publish_view import PublishViewService
from app.services.storage_tier import StorageTierService

router = APIRouter(prefix="/api/tier", tags=["存储分层迁移"])

_storage = StorageTierService()
_publish = PublishViewService()


class MigrationRunPayload(BaseModel):
    lake_table: str
    compression: str = "SNAPPY"
    file_ids: list[str] | None = None
    hot_days: int | None = None
    cold_bytes_ratio: float = 0.4
    idempotency_key: str | None = None
    note: str = ""
    run_pipeline: bool = True


class ViewRefreshPayload(BaseModel):
    lake_table: str
    view_snapshot: str | None = None
    consumer: str | None = None


class ReceiptPayload(BaseModel):
    read_id: str | None = None


class ReconcilePayload(BaseModel):
    migration_id: str
    lake_table: str | None = None
    read_id: str | None = None


@router.get("/tables/{lake_table}/detail")
def tier_detail(lake_table: str) -> dict[str, Any]:
    """存储分层详情：逻辑文件代际位置、清单版本历史与冷层定位凭证。"""
    return _storage.tier_detail(lake_table)


@router.post("/migrations/run")
def run_migration(payload: MigrationRunPayload) -> dict[str, Any]:
    """冷层消费上线前的迁移流水线；同一份迁移输入可幂等重放。"""
    try:
        if payload.run_pipeline:
            return _storage.run_migration_pipeline(
                payload.lake_table,
                payload.compression,
                file_ids=payload.file_ids,
                hot_days=payload.hot_days,
                cold_bytes_ratio=payload.cold_bytes_ratio,
                idempotency_key=payload.idempotency_key,
            )
        return _storage.migrate_cold_files(
            payload.lake_table,
            payload.compression,
            file_ids=payload.file_ids,
            hot_days=payload.hot_days,
            cold_bytes_ratio=payload.cold_bytes_ratio,
            idempotency_key=payload.idempotency_key,
            note=payload.note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/migrations")
def list_migrations(lake_table: str | None = None) -> dict[str, Any]:
    return {
        "items": _storage.list_migrations(lake_table),
        "total": len(_storage.list_migrations(lake_table)),
    }


@router.get("/migrations/{migration_id}")
def get_migration(migration_id: str) -> dict[str, Any]:
    migration = _storage.get_migration(migration_id)
    if migration is None:
        raise HTTPException(status_code=404, detail="迁移记录不存在")
    return migration


@router.post("/migrations/{migration_id}/receipt")
def record_receipt(migration_id: str, payload: ReceiptPayload | None = None) -> dict[str, Any]:
    """迁移回执补到：确认迁移并归账。"""
    result = _storage.record_receipt(migration_id)
    if not result.get("success") and result.get("error") == "migration not found":
        raise HTTPException(status_code=404, detail="迁移记录不存在")
    return result


@router.post("/migrations/{migration_id}/cancel")
def cancel_migration(migration_id: str) -> dict[str, Any]:
    """取消本轮转换并还原旧版清单。"""
    result = _storage.cancel_migration(migration_id)
    if not result.get("success") and result.get("error") == "migration not found":
        raise HTTPException(status_code=404, detail="迁移记录不存在")
    return result


@router.get("/manifests/{lake_table}")
def list_manifests(lake_table: str) -> dict[str, Any]:
    detail = _storage.tier_detail(lake_table)
    return {
        "lake_table": lake_table,
        "active_manifest_version": detail["active_manifest_version"],
        "versions": detail["manifest_versions"],
    }


@router.post("/views/refresh")
def refresh_view(payload: ViewRefreshPayload) -> dict[str, Any]:
    """消费视图刷新：关联本次实际读取的位置版本。"""
    return _publish.refresh_view(
        payload.lake_table, payload.view_snapshot or "", payload.consumer
    )


@router.post("/views/reconcile")
def reconcile_receipt(payload: ReconcilePayload) -> dict[str, Any]:
    """迁移回执补到后，回接到刷新实际读取的位置版本并归账。"""
    migration = _storage.get_migration(payload.migration_id)
    if migration is None:
        raise HTTPException(status_code=404, detail="迁移记录不存在")
    if payload.lake_table and payload.lake_table != migration["lake_table"]:
        raise HTTPException(status_code=400, detail="迁移记录不属于该湖表")
    result = _publish.reconcile_receipt(
        migration["lake_table"], payload.migration_id, payload.read_id
    )
    if not result.get("success"):
        raise HTTPException(status_code=409, detail=result.get("error"))
    return result


@router.get("/views/reads")
def list_view_reads(lake_table: str) -> dict[str, Any]:
    items = _publish.list_reads(lake_table)
    return {"items": items, "total": len(items)}
