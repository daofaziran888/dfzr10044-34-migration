import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ----------------------------- 存储分层迁移与消费视图 -----------------------------

export type TierLocator = {
  logical_file_id: string
  generation: number
  tier: string
  file_id: string
  file_name: string
  locator: string
  rows: number
  bytes: number
  is_active_location: boolean
}

export type TierManifestEntry = {
  logical_file_id: string
  generation: number
  locator: string
  rows: number
  bytes: number
  tier: string
  file_id: string
}

export type TierManifestVersion = {
  version_id: string
  version_number: number
  status: string
  entry_count: number
  migration_ids: string[]
  previous_version_id: string | null
  created_at: number
  entries: TierManifestEntry[]
}

export type TierMigrationRecord = {
  migration_id: string
  lake_table: string
  logical_file_id: string
  source_file_id: string
  source_generation: number
  target_generation: number
  target_file_id: string
  source_bytes: number
  target_bytes: number
  rows: number
  state: string
  receipt_state: string
  savings_bytes: number
  savings_accounted: boolean
  manifest_version: string
  source_locator: string | null
  target_locator: string | null
  target_file_present: boolean
}

export type TierDetail = {
  lake_table: string
  active_manifest_version: string | null
  manifest_versions: TierManifestVersion[]
  files: TierLocator[]
  migrations: TierMigrationRecord[]
  accounted_savings: number
}

export async function getStorageTierDetail(entryId: number): Promise<{
  detail: TierDetail | null
  [field: string]: unknown
}> {
  const response = await request(`/api/storage_tier/${entryId}`)
  if (!response.ok) throw new Error('存储分层详情读取失败')
  return await response.json()
}

export async function refreshConsumptionView(
  lakeTable: string,
  viewSnapshot: string,
): Promise<{
  success: boolean
  read_id?: string
  manifest_version?: string
  refreshed_snapshot?: string
  total_rows?: number
  read_location_versions?: number
  pending_receipts?: string[]
  error?: string
}> {
  const response = await request('/api/tier/views/refresh', {
    method: 'POST',
    body: JSON.stringify({ lake_table: lakeTable, view_snapshot: viewSnapshot, consumer: 'ADHOC_QUERY' }),
  })
  return await response.json()
}

export async function reconcileMigrationReceipt(
  lakeTable: string,
  migrationId: string,
  readId?: string,
): Promise<{
  success: boolean
  read_id?: string
  location_matched?: boolean
  manifest_version?: string
  error?: string
}> {
  const response = await request('/api/tier/views/reconcile', {
    method: 'POST',
    body: JSON.stringify({ migration_id: migrationId, lake_table: lakeTable, read_id: readId }),
  })
  return await response.json()
}

export async function listTierMigrations(lakeTable: string): Promise<TierMigrationRecord[]> {
  const response = await request(`/api/tier/migrations?lake_table=${encodeURIComponent(lakeTable)}`)
  if (!response.ok) throw new Error('迁移记录读取失败')
  const payload = await response.json()
  return payload.items ?? []
}

export async function cancelTierMigration(migrationId: string): Promise<{
  success: boolean
  restored_manifest_version?: string | null
  error?: string
}> {
  const response = await request(`/api/tier/migrations/${migrationId}/cancel`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}
