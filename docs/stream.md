# 事件时间漂移监测管道（init / apply / export / status）

既有 `analyze` 只能比较两份离线频数快照：多个采集源到达不齐时，调用方无法判断某个
时间窗口是否真的完整。本管道持续接收分类事件与各源水位，以**全体配置源的最小水位 G**
作为完整性边界，只关闭已被所有源承诺完整的窗口，把窗口频数交回现有 `analyze`，
并把最终报告可靠交付到本地文件。仅用 Python 标准库与 SQLite；不做分区修订链、
实体配对或人工裁决。

## 1. 数据契约

### 1.1 配置（init）

严格 JSON，**恰好**四个字段：

```json
{"sources": ["sensor-a", "sensor-b"], "categories": ["a", "b", "c"], "alpha": 1, "width": 10}
```

* `sources`：1..64 个唯一 `common.ident` 字符串；
* `categories`、`alpha`：完全沿用 `model.analyze` 契约（1..1000 个唯一类别，
  alpha 为 1..10⁶ 严格正整数）；
* `width`：1..10⁶ 严格整数（`type(x) is int`，布尔拒绝）。

配置在 init 时写入该库 `meta` 表并**固定**：以相同配置重跑 init 是幂等空操作
（`status=unchanged`，进度不丢），不同配置拒绝。apply/export/status 重开数据库
即可继续，不需要再传配置。

### 1.2 事件流（apply）

UTF-8 JSONL，一次性顺序读取（支持不可 seek 的生成器），每行一个严格对象：

```json
{"source": "sensor-a", "seq": 7, "kind": "event", "ts": 42, "category": "a"}
{"source": "sensor-a", "seq": 8, "kind": "watermark", "time": 50}
```

* 事件恰好 `source/seq/kind/ts/category` 五字段，水位恰好 `source/seq/kind/time`
  四字段；未知字段、缺字段拒绝。
* `source` 必须已配置；`category` 必须在声明域；`seq` 为 1..10¹² 严格整数；
  `ts`/`time` 为 0..10¹² 严格整数（布尔、浮点拒绝）。
* 沿用严格 JSON：空行/纯空白行、重复键、`NaN`/`Infinity`、非 UTF-8、
  单行超过 65536 字节均拒绝。单次 apply 最多 10⁷ 行，可跨任意多批次运行。

## 2. 源接纳状态机

每个源独立维护：已提交序号 `committed_seq`（从 1 连续接纳）、水位（初始 0，
不可下降）、接纳事件数、late 数。

* **序号连续**：新行 `seq` 必须等于 `committed_seq+1`，缺口整次 apply 失败。
* **重放判等**：任意批次可重放已提交序号。解析后的规范字段值与类型
  （kind、ts/time、category）与账本完全相同才幂等跳过——JSON 空白与键顺序无关；
  同序号不同载荷抛 `PayloadConflict`，整次 apply 回滚。重放只核对历史载荷，
  **绝不**按当前水位把过去已接纳事件改判为 late。
* **迟到隔离**：新事件 `ts < 该源处理到此处的水位` 时记为 late：仍消耗序号，
  在 `record` 表保留 source、seq、ts、category 与原因，但**绝不计入任何窗口**；
  `ts == 水位` 仍接纳。水位只升不降，新水位行 `time < 当前水位` 整次拒绝。
* **顺序保证**：不同源可任意交错；处理严格按输入顺序（每源顺序即输入顺序），
  从不按事件时间重排。late 判定用的是该源"处理到此处"的水位，因此同批
  "先水位后事件"的迟到判定与跨批一致。

## 3. 窗口关闭与报告

### 3.1 完整性边界

`G = min(各源水位)`。只有 `E = k*width、k >= 2 且 E <= G` 的窗口会关闭：
reference 区间 `[E-2*width, E-width)`，current 区间 `[E-width, E)`，均为半开。
某源水位推进只有**改变全局最小水位**时才可能驱动新窗口关闭；其他源尚未承诺
完整时，快源提供的远期事件只进入区间桶，绝不提前出报告。

### 3.2 恰好一次与空窗口

`meta.next_window_end` 是持久化的关闭前沿（初始 `2*width`）。每次 apply 提交前，
把前沿算术推进到 `floor(G/width)*width`：**空窗口不枚举、不物化**——用
`bucket` 表上的 UNION 查询只挑出"reference 或 current 区间桶存在"的窗口
（`k+1` 与 `k+2` 两个投影），跨 10⁸ 个空窗口也是 O(非空窗口数)。

* 两区间均无事件：不生成文件（也不写报告行）；
* 其余窗口**恰好生成一次**：报告行与前沿推进在同一事务提交，重放、late 事件、
  后续批次都不可能改写已关闭窗口；
* 事件不是实体：同源不同 seq 的相同观察分别计数。

### 3.3 报告内容

窗口关闭时**一次性序列化**并存入 `report.report_json`，之后永远按存储字节发布，
恢复时绝不用当前可变计数重算旧报告。报告含：窗口边界、width、alpha、完整声明域、
关闭时的最小水位与各源序号/水位前缀进度、两侧**完整频数**（含零计数类）与总量、
`distribution_status` 与 `distribution`。

* 两侧总量均为正：以完整声明域和同一 alpha 复用 `model.analyze`，保留精确分数
  p/q 与双零类平滑，`distribution_status="ok"`；
* 任一侧为空：`distribution=null`、`distribution_status="insufficient_population"`，
  不塞伪计数（`analyze` 契约本身拒绝零总量快照）；
* 任一类窗口计数超过 `analyze` 上限 10⁹：整次 apply 报错回滚，不截断、不改变
  旧模型语义。

## 4. 事务边界与崩溃恢复

### 4.1 apply：一个事务

每次 apply 的全部写入——各源 `committed_seq`/水位/计数、`record` 接纳账本
（含 late 审计）、区间桶 UPSERT、窗口关闭、待交付报告插入、前沿推进——都在
**同一个 `BEGIN IMMEDIATE` 事务**内提交。无效行即使出现在输入末尾，也回滚整次
apply：源进度、late 审计、已触发的窗口与报告全部不留痕迹。apply 只提交接纳与
报告生成，**不公开任何文件**，因此未提交内容不可能被 export 或 status 当作有效报告。

### 4.2 export：提交与发布分离

数据库提交与文件落盘之间没有跨介质原子事务，export 按 `window_end` 升序逐个：

1. 从 `report` 表取 `delivered=0` 的报告（存储的原始字节）；
2. 写同目录临时文件 + fsync + `os.replace` 原子发布为 `OUTPUT_DIR/window-END.json`
   ——公开文件绝不出现半份 JSON；
3. 在独立小事务里 `UPDATE delivered=1`（交付确认）。

| 崩溃点 | 重启后 export 行为 |
|---|---|
| 报告提交后、文件替换前 | 目标不存在，重新发布同一批存储字节 |
| 文件已替换、确认前 | 目标字节与库存相同 → 视为成功，仅补确认，**不产生第二份逻辑报告** |
| 确认后 | 已交付，跳过 |

目标已有**不同字节**或是**符号链接**：报错退出（退出码 2），原文件保留，
绝不把异物覆盖成成功；运维删除异物后重跑 export 即可补齐。已替换文件的字节
与库存报告逐字节一致，因为发布用的就是关闭时序列化的存储字节。

只承诺同一库单写者与进程崩溃恢复（SQLite 回滚日志保证单事务原子性），
不承诺断电保证。

## 5. 历史保留与可丢弃

| 数据 | 保留策略 | 原因 |
|---|---|---|
| `record`（每源每序号一行，含规范载荷与处置） | **永久保留** | 任意旧序号都可能在后续批次重放，判等需要历史载荷；late 行同时是审计流 |
| `bucket`（区间×类别稀疏计数） | 窗口关闭后**可剪枝** | 桶 k 只喂窗口 `(k+1)*width`、`(k+2)*width`；两者关闭后（`k <= jmax-2`）即删除。新事件 `ts >= 源水位 >= G`，永不落入已剪枝区间 |
| `report`（含已交付） | **永久保留** | 待交付报告是崩溃恢复的唯一事实来源；已交付行是交付台账与 status 计数来源 |
| `meta`/`source` | 永久 | 配置、关闭前沿、各源进度 |

SQLite 与私有磁盘允许随历史增长；export 的临时文件以 `.` 为前缀写在目标同目录，
替换即消失，崩溃残留可手工删除，永远不会被当作报告。

## 6. 资源界限

* Python 侧只保留：有界批次缓冲（每 8192 行刷库一次，含待判等映射）、
  ≤64 个源的标量状态、≤1000 个类别域、单条报告文本与流式游标；
  不装载与累计事件数、序号数或报告数成正比的集合。
* 所有事件/序号/报告比例的状态都在 SQLite；`iter_late`、status、export
  全部流式读取（export 每次 `LIMIT 1` 取一个待交付报告）。
* 空窗口按前沿算术跳过，不按巨大时间跨度枚举。
* 单次 apply ≤10⁷ 行；批次数不限。

## 7. CLI 与恢复办法

```sh
python -m distribution_drift.stream_cli init   --db DB --config CONFIG.json
python -m distribution_drift.stream_cli apply  --db DB --input EVENTS.jsonl
python -m distribution_drift.stream_cli export --db DB --output-dir DIR
python -m distribution_drift.stream_cli status --db DB
```

* `init`：固定配置；同配置重跑幂等（`unchanged`），不同配置拒绝。
* `apply`：原子接纳一批 JSONL，输出行数、重放数、接纳/late/水位数、
  关闭窗口数与生成报告数；任何无效行整批回滚。同一文件可安全重复 apply
  （全部行按重放判等处理）。
* `export`：把全部已提交报告按 end 升序发布为 `window-END.json`，可重复运行。
  **提交成功但交付失败**（进程在确认前退出、目标出现异物）时：直接重跑
  `export`；同字节自动确认，异物/符号链接报错并保留，删除后重跑补齐。
* `status`：输出固定配置、各源序号/水位/接纳/late、全局最小水位、下一窗口端、
  待交付/已交付报告数。late 审计用流式 API `Store.iter_late([source])`
  按 (source, seq) 顺序读取。

任何契约/冲突/交付/IO 错误以退出码 2 与 stderr 消息返回，已提交状态绝不半写。

## 8. 限额汇总

| 对象 | 上限 |
|---|---|
| 源数量 | 1..64，唯一非空 ident |
| 类别域 | 1..1000，唯一非空 ident |
| alpha | 1..10⁶ 严格整数 |
| width | 1..10⁶ 严格整数 |
| seq | 1..10¹² 严格整数，每源从 1 连续 |
| ts / time | 0..10¹² 严格整数，水位不可下降 |
| 单行 | ≤65536 字节，严格 JSON |
| 单次 apply | ≤10 000 000 行 |
| 窗口每类计数 | ≤10⁹（`analyze` 上限），超出整次 apply 报错 |

## 9. 测试

`tests/test_stream.py` 覆盖：配置与严格行契约；跨批次旧序号重放、载荷冲突、
序号缺口、水位下降、边界 ts 等于水位、同批先水位后迟到、重放不改判、交错源、
批内未刷库重放、刷库后回滚；半开边界、双零类、单侧为空、双空窗不出文件、
10⁸ 空窗口算术跳过、窗口恰好一次、超 analyze 计数上限整批失败、已关闭报告不可改写；
独立小样本 oracle 随机核对频数、late 流、关闭水位，并与 `analyze` 完全一致及
独立 Fraction 计算 PSI/JS；真实子进程交付崩溃（提交后退出、替换后确认前退出、
目标异物、符号链接），恢复字节与不中断执行一致；width=10 慢源闸门联动场景
（含末行错误整批回滚）；固定夹具逐字节比对；CLI 全链路与失败保留。
