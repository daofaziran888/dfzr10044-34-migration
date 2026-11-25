<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in availableActions(row)"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="meta.key === 'storage_tier'"
              class="link"
              type="button"
              data-testid="action-open-tier-detail"
              @click="openTierDetail(row)"
            >
              查看详情
            </button>
            <button
              v-if="meta.key === 'storage_tier' && tierCanCancel(row)"
              class="link"
              type="button"
              data-testid="action-cancel-tier-migration"
              @click="cancelLatestMigration(row)"
            >
              取消本轮迁移
            </button>
            <button
              v-if="meta.key === 'publish_view'"
              class="link"
              type="button"
              data-testid="action-refresh-view"
              @click="refreshView(row)"
            >
              刷新读取
            </button>
            <button
              v-if="meta.key === 'publish_view' && pendingReceiptFor(row)"
              class="link"
              type="button"
              data-testid="action-reconcile-receipt"
              @click="reconcileReceipt(row)"
            >
              回执归账
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section v-if="meta.key === 'publish_view' && refreshNotice" class="notice" data-testid="view-refresh-notice">
      <strong>最近刷新</strong>
      <span>{{ refreshNotice }}</span>
    </section>

    <section v-if="meta.key === 'storage_tier' && tierDetail" class="tier-detail" data-testid="tier-detail">
      <header class="tier-detail-head">
        <div>
          <h3>存储分层详情 · {{ tierDetail.lake_table }}</h3>
          <p class="page-desc">
            当前清单版本 {{ tierDetail.active_manifest_version ?? '—' }}，已归账节省
            {{ tierDetail.accounted_savings }} 字节
          </p>
        </div>
        <button class="btn ghost" type="button" data-testid="close-tier-detail" @click="tierDetail = null">
          关闭详情
        </button>
      </header>

      <h4>物理文件代际位置</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>逻辑文件</th><th>代际</th><th>层级</th><th>物理文件</th>
            <th>定位凭证</th><th>行数</th><th>字节</th><th>当前清单位置</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="file in tierDetail.files" :key="file.file_id" :data-testid="`tier-file-${file.file_id}`">
            <td>{{ file.logical_file_id }}</td>
            <td>{{ file.generation }}</td>
            <td>{{ file.tier }}</td>
            <td>{{ file.file_id }}</td>
            <td class="locator" data-testid="tier-locator">{{ file.locator }}</td>
            <td>{{ file.rows }}</td>
            <td>{{ file.bytes }}</td>
            <td>{{ file.is_active_location ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>

      <h4>迁移记录</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>迁移编号</th><th>逻辑文件</th><th>源代</th><th>目标代</th>
            <th>源字节</th><th>冷层字节</th><th>状态</th><th>回执</th><th>清单版本</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="migration in tierDetail.migrations" :key="migration.migration_id">
            <td>{{ migration.migration_id }}</td>
            <td>{{ migration.logical_file_id }}</td>
            <td>{{ migration.source_generation }}</td>
            <td>{{ migration.target_generation }}</td>
            <td>{{ migration.source_bytes }}</td>
            <td>{{ migration.target_bytes }}</td>
            <td>{{ migration.state }}</td>
            <td>{{ migration.receipt_state }}</td>
            <td>{{ migration.manifest_version }}</td>
          </tr>
          <tr v-if="!tierDetail.migrations.length">
            <td colspan="9" class="empty-state">暂无迁移记录</td>
          </tr>
        </tbody>
      </table>

      <h4>存量清单版本</h4>
      <table class="data-table">
        <thead>
          <tr><th>版本</th><th>序号</th><th>状态</th><th>条目数</th><th>迁移</th><th>上一版</th><th>条目位置</th></tr>
        </thead>
        <tbody>
          <tr v-for="version in tierDetail.manifest_versions" :key="version.version_id">
            <td>{{ version.version_id }}</td>
            <td>{{ version.version_number }}</td>
            <td>{{ version.status }}</td>
            <td>{{ version.entry_count }}</td>
            <td>{{ version.migration_ids.join(', ') || '—' }}</td>
            <td>{{ version.previous_version_id ?? '—' }}</td>
            <td class="locator">
              <span v-for="entry in version.entries" :key="entry.logical_file_id + entry.generation">
                {{ entry.logical_file_id }}@gen{{ entry.generation }} → {{ entry.locator }}（{{ entry.rows }} 行 / {{ entry.bytes }} 字节）
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  cancelTierMigration,
  createEntry,
  downloadEntries,
  getStorageTierDetail,
  listEntries,
  listTierMigrations,
  moduleMeta,
  reconcileMigrationReceipt,
  refreshConsumptionView,
  runAction as applyAction,
} from '@/api/backend-service'
import type { TierDetail } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const tierDetail = ref<TierDetail | null>(null)
const refreshNotice = ref('')
const pendingReceipts = ref<Record<string, string[]>>({})
const lastReadIds = ref<Record<string, string>>({})

function availableActions(row: EntryRow): string[] {
  return meta.actions
}

function tierCanCancel(row: EntryRow): boolean {
  const lakeTable = String(row['所属湖表'] ?? '')
  if (!lakeTable) return false
  // 详情未展开时也允许直接取消：取消动作会按实际活动清单寻找可回退的迁移
  if (!tierDetail.value || tierDetail.value.lake_table !== lakeTable) return true
  return tierDetail.value.migrations.some((migration) => migration.state !== 'canceled')
}

function pendingReceiptFor(row: EntryRow): string | null {
  const lakeTable = String(row['所属湖表'] ?? '')
  const pending = pendingReceipts.value[lakeTable]
  return pending && pending.length ? pending[0] : null
}

async function openTierDetail(row: EntryRow) {
  errorMessage.value = ''
  try {
    const payload = await getStorageTierDetail(Number(row.id))
    tierDetail.value = payload.detail ?? null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '存储分层详情读取失败'
  }
}

async function cancelLatestMigration(row: EntryRow) {
  const lakeTable = String(row['所属湖表'] ?? '')
  errorMessage.value = ''
  try {
    let migrations = tierDetail.value?.lake_table === lakeTable
      ? tierDetail.value.migrations
      : []
    if (!migrations.length) {
      migrations = await listTierMigrations(lakeTable)
    }
    const migration = migrations.find((item) => item.state !== 'canceled')
    if (!migration) {
      errorMessage.value = '当前没有可取消的迁移'
      return
    }
    const result = await cancelTierMigration(migration.migration_id)
    if (!result.success) throw new Error(result.error ?? '取消迁移失败')
    refreshNotice.value = `已取消 ${migration.migration_id}，清单还原到 ${result.restored_manifest_version ?? '上一版'}`
    await openTierDetail(row)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '取消迁移失败'
  }
}

async function refreshView(row: EntryRow) {
  const lakeTable = String(row['所属湖表'] ?? '')
  const snapshot = String(row['绑定快照'] ?? '')
  errorMessage.value = ''
  try {
    const result = await refreshConsumptionView(lakeTable, snapshot)
    if (!result.success) throw new Error(result.error ?? '消费视图刷新失败')
    pendingReceipts.value = {
      ...pendingReceipts.value,
      [lakeTable]: result.pending_receipts ?? [],
    }
    if (result.read_id) {
      lastReadIds.value = { ...lastReadIds.value, [lakeTable]: result.read_id }
    }
    refreshNotice.value = `刷新读取 ${result.refreshed_snapshot}，位置版本 ${result.manifest_version ?? '—'}，共 ${result.total_rows ?? 0} 行`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消费视图刷新失败'
  }
}

async function reconcileReceipt(row: EntryRow) {
  const lakeTable = String(row['所属湖表'] ?? '')
  const migrationId = pendingReceiptFor(row)
  if (!migrationId) return
  errorMessage.value = ''
  try {
    const result = await reconcileMigrationReceipt(lakeTable, migrationId, lastReadIds.value[lakeTable])
    if (!result.success) throw new Error(result.error ?? '回执归账失败')
    refreshNotice.value = `回执 ${migrationId} 已归账到读取 ${result.read_id ?? '—'}，位置匹配 ${result.location_matched ? '是' : '否'}`
    pendingReceipts.value = {
      ...pendingReceipts.value,
      [lakeTable]: (pendingReceipts.value[lakeTable] ?? []).filter((id) => id !== migrationId),
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执归账失败'
  }
}
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
