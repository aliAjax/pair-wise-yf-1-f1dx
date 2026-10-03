# 冰码头预约取冰服务

从零实现、**不装任何第三方依赖**即可启动的 HTTP 服务。值班员用 HTTP 完成建预约、确认取冰、查结欠。

- 语言：Python 3 标准库（`http.server` + `sqlite3`）
- 存储：SQLite 单文件（WAL 模式，断电不丢）
- 并发：`ThreadingHTTPServer` + `BEGIN IMMEDIATE` 串行写 + 唯一约束幂等

## 启动

```bash
python3 -m ice_dock --host 127.0.0.1 --port 8765 --db ice_dock.db
```

服务启动后监听 `http://127.0.0.1:8765`，后台线程同步 outbox。

## 演示

```bash
python3 demo.py
```

脚本以子进程启动服务，端到端验证下述全部场景。

## 需求与实现对照

| 需求 | 实现 |
|------|------|
| 船早到晚到撞上原时段 | 确认时可传 `actual_slot_id`，跨时段释放原时段、占用实际时段并重查容量 |
| 过磅实拿少于预约 | `actual_amount` 与 `booked_amount` 之差记为 `shortfall` |
| 确认时重查余量 | 确认事务内重查实际时段 `capacity - occupied`，超量拒绝 |
| 两个码头员同时提交同一条确认只记一次 | `confirmations.idempotency_key` 与 `appointment_id` 双唯一约束 + `BEGIN IMMEDIATE`，并发提交只落一条 |
| 少拿的差额结到下一航次 | `balances` 按船主累计 `shortfall`，自然结转下一航次 |
| 码头断网时预约照常记 | 网络断开时写操作照常落本地 SQLite，并写 outbox（状态 `pending`） |
| 恢复后重发不重复占容量 | 后台线程重发 outbox，业务层幂等（唯一约束）保证不重复占容量 |
| 容量满时新预约先排队 | `occupied + amount > capacity` 时状态置 `queued` 不占容量；容量释放后 FIFO 补位 |
| 制冰机停机时正在处理的预约整批退回队列 | `POST /ice-maker/stop` 将所有 `processing` 预约批量置回 `queued` 并提交 |

## 核心概念

### 预约状态机

```
queued ──promote──▶ booked ──start──▶ processing ──complete──▶ confirmed
  ▲                   │                  │
  └─── ice-maker stop ┘                  │
                                        ▼
                                   shortfall → balances
```

- `queued`：容量满，排队中，不占容量
- `booked`：已占容量
- `processing`：正在取冰（制冰机停机时这批整批退回 `queued`）
- `confirmed`：取冰完成，记录实拿与差额

### 幂等

所有写操作带 `idempotency_key`。重发（网络恢复重发、码头员重复提交、并发提交）时：

1. 事务内先查唯一键是否已存在；
2. 已存在则直接返回原记录（`duplicated=true`），不再执行容量变更；
3. 唯一约束兜底，`BEGIN IMMEDIATE` 串行化并发。

### 离线 outbox

```
写请求 ──▶ 本地 SQLite（照常记）──▶ outbox(pending)
                                        │
                          后台线程每 1s 检查 network=up?
                                        │ 是
                                        ▼
                          重发业务层（幂等）──▶ outbox(synced)
```

- `network=down`：后台线程暂停重发，本地照常写；
- `network=up`：恢复重发，唯一约束保证不重复占容量；
- 手动触发：`POST /outbox/replay`。

## HTTP 接口

### 健康
```
GET  /health
```

### 时段
```
POST /slots                {"slot_id":"S1","slot_time":"08:00-10:00","capacity":100}
GET  /slots
```

### 预约
```
POST /appointments         {"boat_owner":"船主A","slot_id":"S1","amount":60,"idempotency_key":"apt-001","voyage_id":"V1"}
GET  /appointments?boat_owner=船主A&status=booked
GET  /appointments/{id}
```

### 取冰确认
```
POST /confirmations        {"appointment_id":"...","actual_amount":50,"idempotency_key":"cnf-001","actual_slot_id":"S1"}
POST /confirmations/start  {"appointment_id":"..."}
POST /confirmations/complete {"appointment_id":"...","actual_amount":50,"idempotency_key":"cnf-001"}
GET  /confirmations?boat_owner=船主A
```

### 结欠
```
GET  /balances
GET  /balances/船主A
```

### 队列
```
GET  /queue
```

### 制冰机
```
GET  /ice-maker
POST /ice-maker/start
POST /ice-maker/stop       # 正在处理的预约整批退回队列
```

### 网络与 outbox
```
GET  /network
POST /network/up
POST /network/down
GET  /outbox?status=pending
POST /outbox/replay
```

## 文件结构

```
ice_dock/
  __init__.py     包导出
  __main__.py     入口：python -m ice_dock
  db.py           SQLite schema、连接、初始化
  service.py      业务逻辑（预约/确认/结欠/排队/制冰机/网络/outbox）
  server.py       HTTP Handler + ThreadingHTTPServer
  worker.py       outbox 后台同步线程
demo.py           端到端演示
```

## 设计说明

- **为什么用 SQLite 而不是内存**：断网/重启后预约不丢，`BEGIN IMMEDIATE` 与唯一约束天然适合幂等与批量回退。
- **为什么 outbox 与业务数据同库**：本地写与 outbox 在同一事务提交，保证「照常记」与「待同步」一致，不会出现记了但没发的中间态。
- **确认拆成 start/complete 两阶段**：`processing` 状态落库后，制冰机停机才能把「正在处理」的预约整批退回；单阶段原子操作没有这个窗口。`POST /confirmations` 做 start+complete 一站式处理，`start`/`complete` 接口用于演示两阶段。
