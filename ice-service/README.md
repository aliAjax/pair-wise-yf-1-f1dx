# 冰港取冰预约服务（零依赖）

一个**只依赖 Python 3.11+ 标准库**、单文件即可启动的 HTTP 服务，供值班员为船东
按到港时段预约取冰、过磅确认、查询结欠。无数据库、无第三方包；数据落在一个
JSONL 追加日志里，重启自动恢复。

## 启动

```bash
python3 ice_service.py                       # 默认 127.0.0.1:8080，日志 ice_journal.log
python3 ice_service.py --port 9000 --log /var/lib/ice/j.log
# 或用环境变量 ICE_HOST / ICE_PORT / ICE_LOG
```

健康检查：`GET /` → `{"service":"ice-service","status":"ok"}`

## 快速验证

```bash
python3 test_ice_service.py     # 10 个单元/并发/恢复测试
python3 smoke_demo.py           # 自动拉起临时服务，把完整业务场景走一遍
```

## 业务规则如何落地

| 题目要求 | 实现方式 |
| --- | --- |
| 船主按到港时段预约取冰 | 每个时段(slot)有制冰容量；预约占用容量，返回 `reserved` |
| 容量满时新预约先排队 | 余量不足或时段已有排队者（严格 FIFO、不许插队）时返回 `202 queued`；容量一释放（确认/取消/开机）自动从队首放号，队首放不下则阻塞其后 |
| 早到/晚到撞上别的时段 | 确认时可带 `actual_slot_id`；服务**现场重查**实际时段余量，不足返回 409；跨槽确认不占原时段名额，并从原队列移除、触发原队列放号 |
| 确认时重查余量 | 确认路径上余量全部即时重算（`容量 − 已占容量 − 已过磅交付`），不使用预约时的旧快照 |
| 两码头员同时提交同一条确认只记一次 | 确认必须带 `confirm_key`（建议终端单号），服务对其去重；并发请求被同一把锁串行化，后到者原样返回第一次结果（`deduplicated=true`） |
| 过磅实拿少于预约，差额结到下一航次 | 本航次自身需求未满足的部分记入船东台账（shortfall）；下一航次预约量必须覆盖结欠，否则 409；过磅后优先满足本航次，多出部分自动偿还历史结欠（repayment） |
| 码头断网时预约照常记 | 请求先 `write + fsync` 落日志，再改内存并应答，崩溃/断电不丢 |
| 网络恢复后重发不重复占容量 | 预约带 `client_key`、确认带 `confirm_key`；重启后幂等键随日志恢复，重发永远只生效一次 |
| 制冰机停机，处理中预约整批退回队列 | `POST /slots/{id}/machine {"running":false}` 把所有 `reserved` 预约按序成块退回队首；停机期间不能确认；开机后按 FIFO 重新入场 |

结欠记账口径：

```
本航次新增结欠 = max(0, 本航次自身需求 − 实拿)
本次偿还结欠   = max(0, min(带入结欠, 实拿 − 本航次自身需求))
船东结余       = Σ shortfall − Σ repayment      （GET /ships/{船东} 可查）
```

其中 `本航次自身需求 = 预约量 − 预约时带入的结欠`（带入结欠是用来"还旧账"的额外冰量）。

## HTTP 接口

### 时段与制冰机

```
POST /slots                         {"slot_id":"AM","capacity":100}      → 201
GET  /slots                                                             → 时段列表
GET  /slots/{slot_id}                    含 capacity/reserved/delivered/remaining/queue
POST /slots/{slot_id}/machine        {"running":false}                   停机/开机
```

### 预约（值班员/船东）

```
POST /bookings
{
  "slot_id": "AM",          # 预约的到港时段
  "ship": "沪渔1",          # 船东（结欠按船东归集）
  "voyage": "V1",           # 航次号
  "qty": 30,                # 预约量（含覆盖历史结欠的部分）
  "client_key": "TERM-7"    # 终端幂等键：断网重发用同一个，必填建议由终端生成
}
→ 201 reserved（已占容量） / 202 queued（排队中）
GET /bookings/{booking_id}
POST /bookings/{booking_id}/cancel
```

### 确认取冰（码头过磅处）

```
POST /bookings/{booking_id}/confirm
{
  "delivered_qty": 25,      # 过磅实拿
  "confirm_key": "TICKET-1",# 终端单号，同一单号无论几个码头员点几次都只记一次
  "actual_slot_id": "PM"    # 可选：船实际到的时段（早到/晚到）；缺省为原时段
}
```

返回示例（少拿 + 偿还）：

```json
{
  "deduplicated": false,
  "confirmation": {
    "booking_id": "B-…", "booked_slot": "PM", "actual_slot": "PM",
    "booked_qty": 25.0, "delivered_qty": 22.0,
    "shortfall_qty": 3.0,
    "new_arrear": 0.0, "arrear_repaid": 2.0,
    "arrear_carried_in": 5.0,
    "ship_balance": 3.0,
    "confirm_key": "TICKET-2"
  }
}
```

### 结欠

```
GET /ships/{ship}   → {"ship":…, "balance":尚欠总量, "entries":[台账流水…]}
```

## 数据与并发

- `ice_journal.log`：每行一个 JSON 事件（建槽、预约、入场、退回、确认、取消、台账），
  追加写并 `fsync`；启动时回放重建全部状态（含两条幂等键索引）。
- 全进程一把可重入锁串行化所有写事务；HTTP 用 `ThreadingHTTPServer`，
  读请求不阻塞、写请求排队，保证"同一条确认只记一次"和队列状态的一致性。
- 数值用浮点 + ε 容差比较，重量单位与业务保持一致即可。

## 断网/故障操作指引（值班员）

1. 终端请求超时/断网时，**保留原始单号**（预约 `client_key`、确认 `confirm_key`）直接重发；
   服务恢复后会返回 `deduplicated:true`，容量不会被扣两次。
2. 服务进程重启无需任何恢复命令，日志在则状态在。
3. 制冰机故障先调停机接口，处理中的预约自动回队；排除故障后开机即自动放号。
