# 水质数仓单实例 PITR 恢复服务

面向"值班员误删样品后，要取回删前核对时点数据、同时保留生产库后续写入"的场景。
基于 **FastAPI + PostgreSQL 16 + Docker Compose**：

- 生产源库可写样品库（样品号 / 站号 / pH），开启 **WAL 持续归档**；
- API 自动制作一份 **物理基准备份**（`pg_basebackup`，源库不停写）；
- 可对已完成备份执行 **完整性核验**（`pg_verifybackup`，含备份自带 WAL），
  返回损坏明细、核验时间与备份代次；核验失败/中断即阻断新恢复；
- 可随时登记 **命名核对点**（`pg_create_restore_point`）；
- 按核对点 **PITR 恢复到独立只读实例** 并查询样品；
- 恢复结果只含核对点前已提交的数据，**跨点后才提交的事务不会出现**；
- 查询只打恢复实例，**绝不用当前源库冒充恢复结果**；
- WAL 未归档齐备时任务显示等待、不开放目标查询；
- 失败或 API 重启后可按原核对点重试；一次只恢复一项；全程不改写生产库。

## 快速开始

```bash
docker compose up -d --build        # 启动源库 + API（首次自动做基准备份）
python3 scripts/demo.py             # 端到端演示：更正 / 误删 / 跨点事务 + 恢复校验
python3 scripts/verify_demo.py      # 备份核验演示：损坏 / 换备份 / 中断 / 恢复阻断
```

Swagger 文档：<http://localhost:8000/docs>

`verify_demo.py` 需要能直接改写备份目录才能人为造损坏，默认会通过
`docker volume inspect` 自动找备份卷的宿主机路径；也可显式指定：

```bash
BACKUP_VOLUME=$(docker volume inspect -f '{{.Mountpoint}}' water-pitr_backups) \
  python3 scripts/verify_demo.py
```

演示脚本依次验证（`demo.py`）：

| 场景 | 操作 | 恢复校验 |
| --- | --- | --- |
| 更正 | `UPDATE` 修改 S-2026-0002 的 pH | cp1 中仍是旧值 6.95，cp3 中是新值 7.55 |
| 误删 | `DELETE` 删除 S-2026-0003 | cp1 中查得到，cp3 中已删除 |
| 跨点事务 | 事务在 cp3 前 `INSERT`、cp3 后才 `COMMIT` | cp3 恢复结果中**不含**该行 |
| 源库不受打扰 | 恢复进行期间向源库写入新样品 | 写入成功；恢复实例中也看不到该行（证明没拿源库冒充） |

`verify_demo.py` 依次验证：正常备份核验通过并能恢复；人为损坏 → 核验 `failed`
（含损坏文件、核验时间、代次）且新恢复 409，但已就绪实例仍可查；强制换备份 →
新代次未核验仍阻断、核验通过后解闸并恢复成功；核验进行中被中断 → `interrupted`
不报通过、闸门立起，重新核验通过才解除。

## 架构

```
                 ┌──────────────────────────── compose ───────────────────────────┐
                 │                                                                  │
  写入/变更 ───▶ │  db (PostgreSQL 16, 可写)                                        │
                 │    archive_mode=on ──WAL 段(原子改名)──▶ [wal_archive 卷]         │
                 │                                          │ (只读挂载)             │
                 │  api (FastAPI + PG16 工具)               ▼                       │
                 │    ① pg_basebackup ──▶ [/backups/gen-N]  核对 WAL 段齐备          │
                 │       pg_verifybackup ─▶ 核验该代备份 + 自带 WAL                  │
                 │    ② pg_create_restore_point(核对点) ──▶ 源库（仅 WAL 元数据）    │
                 │    ③ 拷贝备份 + recovery.signal ──▶ [/recoveries/job-N]           │
                 │       restore_command 从归档重放 ─▶ pause@核对点 ─▶ 校验 ─▶ 提升  │
                 │    ④ 只读查询 ◀── 恢复实例 127.0.0.1:55433（独立数据目录）        │
                 │                                                                  │
                 └──────────────────────────────────────────────────────────────────┘
```

每代备份落在独立目录 `/backups/gen-<id>`，**不原地覆盖**：强制重做只新增代次，
旧代次的核验与恢复仍在原路径上进行，结论不会串到新代次。

## 关键机制与需求对照

| 需求 | 实现 |
| --- | --- |
| 物理基准备份 + 持续归档 | `pg_basebackup -Fp -X stream`（自洽含备份期 WAL）；源库 `archive_command` 先写临时文件再原子 `mv`，API 只会看到完整 WAL 段 |
| 备份完整性核验 | `pg_verifybackup -w <备份>/pg_wal <备份目录>`：既校验 manifest 中每个文件的 CRC32C，也解析备份自带的 WAL 段。结果落 `verifications` 表：`backup_id`（代次）、`started_at/finished_at`（核验时间）、`corruption`（损坏明细）、`output` |
| 旧结论不归到新备份 | 每代备份独立目录 + 核验记录绑定 `backups.id` + 核验开始时记录 `backup_manifest` 的 SHA-256，结束时复核；强制重做期间旧核验仍写回旧代次，新代次核验记录为空 |
| 核验中断不报通过 | `pg_verifybackup` 正常退出且 manifest 未变才算 `passed`；用户取消（`DELETE /api/verifications/{id}`）、API 重启/关停均记 `interrupted`，并终止遗留进程 |
| 核验失败阻断恢复 | 失败/中断立起"完整性闸门"（`service_meta.integrity_gate_backup`，记录被阻断代次与造成阻断的核验 id）：新恢复请求 409；进行中的恢复在发布 `ready` 前被拦下 → `failed`。此外**该代次自身**永远不得用于发布 ready（即使闸门已被更新备份解除） |
| 已就绪独立只读库仍可查 | 闸门只作用于"新的恢复发布"，不触碰已 `ready` 实例；新恢复启动时旧实例只是**停止**、数据目录保留，若新恢复因判坏失败，旧实例自动拉回恢复 `ready`；API 重启后 ready 实例也照常拉起 |
| 新备份须重新核验才解除 | 只有 **晚于造成阻断的核验**、且 **备份代次高于被阻断代次** 的 `passed` 结论才解闸：必须换一份新备份重新核验；同代次重验或失败前的旧 `passed` 都不能解禁 |
| 登记命名核对点 | `POST /api/checkpoints` → `pg_create_restore_point` + `pg_switch_wal`（促使该段立即归档） |
| 只含核对点前已提交数据 | `recovery_target_name` + `recovery_target_action=pause`，重放精确停在核对点；跨点后提交的事务不重放 |
| 不用源库冒充恢复 | 查询连接的是 `/recoveries/job-N` 下独立实例（端口 55433），响应中标注实例与 as-of LSN |
| WAL 未齐备显示等待 | 核对点 `waiting_wal` 状态；任务逐段核对"备份起点→核对点"全部 WAL 段，缺段时停留 `waiting_wal` 并列出缺失段 |
| 不把半库报成功 | pause 后校验 `pg_last_wal_replay_lsn() >= 核对点 LSN` 才 promote；再查 `samples`  sanity + `default_transaction_read_only=on` 才置 `ready`；任何异常 → `failed` + 日志 |
| 失败/重启可重试 | 任务状态落 SQLite；`POST /api/recoveries/{id}/retry` 按原点重试；API 重启时进行中任务自动重新排队、ready 实例自动拉起 |
| 一次只恢复一项 | 有进行中任务时 `POST /api/recoveries` 返回 409；新任务就绪前把旧 ready 实例停掉并标记 `superseded`（数据目录保留，新任务失败时旧实例自动拉回） |
| 源库可写、不被改写 | 基准备份/恢复点/核验均为在线只读操作；API 对源库不执行任何 DML；归档卷对 API 只读挂载 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查（源库连通性、已归档 WAL 数） |
| POST | `/api/basebackup?force=` | 制作物理基准备份（默认启动时自动做一次；每次产生新代次） |
| GET | `/api/basebackup` | 备份状态（起始 LSN、大小、历史、当前完整性闸门） |
| POST | `/api/basebackup/{backup_id}/verify` | **核验指定备份代次**（`pg_verifybackup`，含自带 WAL），202 异步；返回核验 id、代次、状态 |
| GET | `/api/basebackup/{backup_id}/verify` | 该代次最近一次核验（损坏信息、核验时间、代次） |
| GET | `/api/basebackup/{backup_id}/verifications` | 该代次全部核验记录 |
| GET | `/api/verifications` | 核验记录列表 + 当前完整性闸门（`blocked` / 被阻断代次 / 处置建议） |
| GET | `/api/verifications/{id}` | 核验详情（`corruption`、`output`、`verified_at`、`backup_generation`） |
| DELETE | `/api/verifications/{id}` | 中断进行中的核验（记 `interrupted`，绝不报通过） |
| POST | `/api/checkpoints` | 登记命名核对点 `{"name": "cp1"}` |
| GET | `/api/checkpoints` `/api/checkpoints/{name}` | 核对点状态（`waiting_wal`/`ready`/`expired`） |
| POST | `/api/recoveries` | 按核对点发起恢复 `{"checkpoint": "cp1"}`（一次一项；闸门阻断时 409） |
| GET | `/api/recoveries` `/api/recoveries/{id}` | 任务状态查询（含实际取用的 `backup_id`） |
| POST | `/api/recoveries/{id}/retry` | 失败/取消/被取代后按原点重试 |
| DELETE | `/api/recoveries/{id}` | 取消进行中任务 / 停止已就绪实例 |
| GET | `/api/recoveries/{id}/samples` | 查询恢复实例样品（`station_no`/`sample_no` 过滤，仅 `ready` 后开放） |
| POST | `/api/recoveries/{id}/query` | 只读 SQL（演示用，限单条 SELECT，连接强制只读兜底） |

### 备份核验状态机

```
running → passed          （exit 0 且 manifest 未变）
        → failed          （校验和不符 / WAL 解析失败 / 无法执行）
        → interrupted     （用户取消 / API 重启或关停 / manifest 被替换）

failed / interrupted ⇒ 闸门 = 该备份代次 + 该次核验 id
闸门 → 解除：出现 核验 id 更晚 且 备份代次更高 的 passed 核验
             （即：换一份新备份并重新核验；同代次重验或失败前的旧 passed 都不算）
```

### 恢复任务状态机

```
queued → waiting_wal → preparing → recovering → verifying → ready
           (等待WAL归档齐备)                            ↘ failed（可 retry）
ready --(新任务启动)--> superseded --(新任务失败)--> ready（实例自动拉回）
ready --(DELETE)--> stopped
进行中 --(DELETE)--> cancelled             API 重启：进行中→重新排队；ready→自动拉起
```

## 手动操作示例（curl）

```bash
# 1. 等基准备份完成，拿到代次 id
curl -s localhost:8000/api/basebackup | jq .latest_done

# 2. 核验该代次（含备份自带 WAL），看损坏信息 / 核验时间 / 代次
curl -s -X POST localhost:8000/api/basebackup/1/verify | jq
curl -s localhost:8000/api/basebackup/1/verify | jq '{status, backup_generation, verified_at, corruption}'

# 3. 登记核对点（等 status 变 ready，通常几秒）
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp1"}'
curl -s localhost:8000/api/checkpoints/cp1

# 4. 源库继续变更（更正、误删……）
docker compose exec db psql -U postgres -d water -c "DELETE FROM samples WHERE sample_no='S-2026-0003'"

# 5. 发起恢复并等 ready
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp1"}'
curl -s localhost:8000/api/recoveries/1

# 6. 查询删前时点（S-2026-0003 回来了）
curl -s 'localhost:8000/api/recoveries/1/samples?station_no=ST-02'

# 7. 若核验判坏：观察完整性闸门（恢复会被 409 拒绝）
curl -s localhost:8000/api/verifications | jq .integrity_gate
#    换一份新备份并核验通过后解除
curl -s -X POST 'localhost:8000/api/basebackup?force=true'
curl -s -X POST localhost:8000/api/basebackup/2/verify
```

## 限制与说明

- 演示环境口令为 `postgres/postgres`（scram-sha-256，复制连接已在 `db/init/02_hba.sh` 放行），生产使用请更换口令并收敛 pg_hba；
- 归档卷只增不减，生产需自行配置 WAL 保留/清理策略（如 `pg_archivecleanup` + 备份周期）；
- 重做基准备份（`force=true`）会使早于新备份起点的核对点过期（`expired`）；
- 备份按代次保留在 `/backups/gen-<id>`，不再自动删除旧代次（核验/恢复要按代次追溯）；
  生产需自行配置备份保留策略；
- `superseded` 恢复实例的数据目录同样保留（供新任务失败时回滚拉起），
  成功取代后可由运维按需清理，生产需纳入磁盘配额；
- 核验只证明"备份文件与自带 WAL 完好"，不替代恢复演练；备份起点之后的 WAL 仍依赖归档卷，
  `pg_verifybackup` 不校验归档卷中的后续 WAL；
- 中断核验会立起阻断：需要重新核验（或换新备份并核验）才能恢复，这是有意为之；
- 恢复实例监听容器内 55433，仅 API 经本机 socket 访问，不对外暴露；
- `/query` 为演示便利，真正兜底是连接级 `default_transaction_read_only=on`，任何写操作都会被数据库拒绝。

## 已验证项（PostgreSQL 16.16 实测）

在 PostgreSQL 16.16（PGDG Debian 包，`pg_verifybackup 16.16`）上按 Compose 同拓扑
（源库 + WAL 归档 + API，API 直连本机 PG16）实测通过：

- 冷启动 → 自动基准备份（`gen-1`）→ `POST /api/basebackup/1/verify` → `passed`，
  返回 `backup_generation=1` 与 `verified_at`；
- 核验后恢复核对点 → `ready`、样例可查，任务记录 `backup_id=1`；
- 人为翻转备份中真实数据文件 1 个字节 → 核验 `failed`，`corruption` 精确指出
  `checksum mismatch for file "base/4/4150"`，闸门立起；新恢复请求 409；
- 已 `ready` 的独立只读实例在核验判坏后仍可查询（HTTP 200）；
- 截断备份自带 WAL 段 → `pg_verifybackup` 报 `WAL parsing failed`（退出码 1），按损坏处理；
- 强制重做备份（`gen-2`）→ 新代次核验记录为空、闸门仍阻断；核验 `gen-2` 通过后解闸，
  新核对点恢复 `ready` 且 `backup_id=2`；
- 核验进行中强制重做备份：旧核验结论仍落在 `gen-1`，`gen-2` 无继承结论（代次绑定正确）；
- 核验进行中 `DELETE /api/verifications/{id}` → `interrupted`，不报告通过，闸门立起，恢复 409；
  重新核验通过后才解除；
- 核验进行中 SIGKILL/重启 API → 重启时记 `interrupted` 并清理遗留进程，绝不当作通过；
- 恢复进行中（`recovering`/`verifying`）核验判坏该代次 → 恢复在发布前被拦下，终态 `failed`
  （`该恢复不得发布为 ready`），目标查询 409；
- 同一场景下，先前被本任务停掉的 ready 实例自动拉回并恢复 `ready`（查询 200），
  失败的恢复不会连带毁掉已就绪的独立只读库；
- 同一版本上原有 `scripts/demo.py` 全部断言仍 PASS（更正/误删/跨点事务/恢复期间源库可写/不拿源库冒充）；
- 新增 `scripts/verify_demo.py` 覆盖上述损坏 / 换备份 / 中断 / 阻断场景，全部断言 PASS（可重复执行）；
- 闸门语义单元测试：同代次重验通过不解禁；必须换更高代次备份并核验通过才解禁；
  失败前的旧 `passed`、更旧代次的 `passed` 都不能解禁；多代失败时闸门不降级；
- 逐代封锁单元测试：闸门被更新备份解除后，绑定了被判坏代次的未完成恢复仍不能发布 ready；
  新代次恢复可正常发布；
- 旧状态库（无 `backup_id` / `verifications` / `service_meta`）启动自动补列建表，不影响既有 ready 任务。

未在 Docker Compose 容器拓扑内实测（本机无 Docker 守护进程），仅以同版本 PG16 二进制
按相同目录/端口约定跑通；Compose 配置已同步更新（备份代次目录、`PG_VERIFYBACKUP` 环境变量）。
