# 不可变修订链与封存快照

在既有 `analyze` 平滑漂移诊断之上，增加基于 Python 标准库与 SQLite 的分区增量账本：
迟到更正以"引用前一修订字节 SHA 的修订链"方式追加，已封存报告不可改写、可复现。

## 1. 数据模型与内容身份

每个分片是一次摄取的**原始输入文件字节**，形态为

```json
{"side": "reference|current", "partition": "p0", "revision": 1,
 "parent_sha256": null, "counts": [{"category": "a", "delta": 8}]}
```

* 修订内容身份 = 原始输入字节的 SHA-256，不做 JSON 重排或空白规范化。
  紧凑 JSON 与美化 JSON 即使语义相同也是不同身份。
* `revision` 从 1 开始；首修订 `parent_sha256` 必须为 `null`，其余必须等于
  **同一 side、同一 partition 前一修订号原始输入字节**的 SHA-256。
* 同 `(side, partition, revision)` 重复提交：字节 SHA 相同 → 幂等无操作；
  SHA 不同 → `ConflictError`，账本不变。
* `delta` 必须是严格整数（`type(x) is int`，布尔值拒绝）、非零、绝对值 1..10⁹；
  类别为非空字符串且在单个修订内唯一；每个修订最多 10 000 个非零类别。
* 输入沿用严格 JSON：重复键拒绝、`NaN/Infinity` 拒绝、8 MB 上限。

SQLite 表（均为 `WITHOUT ROWID` 聚簇表）：

| 表 | 作用 |
|---|---|
| `chain(side, partition)` | 已出现过的链（用于每库 100 000 条链上限） |
| `revision(side, partition, revision, parent_sha256, content_sha256, raw)` | 不可变修订正文与哈希，主键即版本身份 |
| `snapshot(snapshot_id, manifest_sha256, manifest_bytes, report)` | 封存身份与完整报告文本 |
| `sealed_revision(snapshot_id, side, partition, revision, content_sha256)` | 每份报告对应的精确修订集合与字节 SHA |

账本没有任何 UPDATE/DELETE 路径；`revision.raw` 只追加，历史链身份因此保持不可变。

## 2. 关键取舍：封存时按需追溯，而非摄取时递归物化

需求允许两种实现，本仓选择**封存时按需追溯父链**。

### 摄取路径（`Store.ingest_bytes`）

摄取只做四件事，全部在一个 `BEGIN IMMEDIATE` 短事务内完成：

1. 严格解析与字段校验（在打开事务之前完成，格式错误完全不触碰数据库）；
2. 按 `(side, partition, revision)` 查主键：同 SHA 幂等返回，异 SHA 冲突回滚；
3. 检查链数（≤100 000）与链内修订数（≤1 000）上限；
4. 插入一行不可变 `revision`。

摄取**不**校验父修订是否已经到达、**不**解析类别域（域属于 manifest，而非分片）、
**不**累计任何频数。因此：

* 修订 3、2 先于修订 1 到达时，3 和 2 作为"悬空修订"正常入账；
* 含负累计的格式合法修订照常保存，链身份与历史可追溯性不受影响；
* 每次摄取 O(1) 次索引查找，不读取任何其他修订正文，不递归、不传播状态。

### 封存路径（`Store.seal_manifest_bytes` → `_trace_prefix`）

封存时对 manifest 引用的每个 `(side, partition, tip)` 做一次流式追溯：

* 按 `revision` 升序打开游标，**每次只保留一行 BLOB 在内存**（不使用 `fetchall`）；
* 行数必须恰好等于 tip，否则存在缺口 → `SealRejected`（补到缺失父修订后，
  同一份 manifest 再次封存即可在一次追溯中解锁全部已存后代）；
* 逐行核对 `parent_sha256 == 前一修订原始字节 SHA`，并重算内容 SHA 与存储值比对；
* 稀疏 `dict` 累计 delta：类别不在 manifest 声明域、任一前缀累计为负、
  累计超过 10⁹ 均拒绝；跨分区合并后再次检查每类 ≤10⁹ 与每侧总量 > 0。

### 为什么不选摄取时递归物化

| 维度 | 摄取时物化（未采用） | 封存时追溯（本实现） |
|---|---|---|
| 迟到父修订 | 需向后代递归重放/重算资格与累计，一次补父可能扇出大量写放大 | 零额外写入，下次封存自然看到完整链 |
| 负累计/越界修订 | 必须把"格式合法但暂不合格"的派生状态单独维护并在补父时翻转 | 不需要派生状态，原始行即唯一事实来源 |
| 类别域 | 分片本身不带域，物化累计无法在摄取时判定越域 | 域随 manifest 进入，在同一封存事务内判定 |
| 恢复 | 崩溃后要重建派生表，否则派生状态与账本可能不一致 | 无派生状态，打开数据库即恢复，无需修复 |
| 代价 | 摄取变重、锁持有时间长 | 代价集中在显式封存：每侧 ≤1000 分区 × 每链 ≤1000 修订，单修订 ≤10 KB 级 JSON，工作量有明确上界；只在封存时支付 |

## 3. 一致性事务与报告提交顺序

* 连接设置 `isolation_level=None`（自动提交）+ 显式 `BEGIN IMMEDIATE`，
  配合 `busy_timeout=30000`。所有写事务在第一个语句即取得保留锁，
  两个独立连接的摄取与封存被 SQLite **串行化**，不会出现写-写死锁。
* 封存事务内完成全部依赖读取（修订链、manifest 身份、历史快照）与全部写入
  （`snapshot`、`sealed_revision`）。不可变修订链、链有效性、快照身份与两侧合并结果
  因此来自**同一一致性边界**：并发补父修订要么在封存取锁前整体可见，
  要么整体不可见，封存不可能观察到半条链。
* 重放判定在同一事务内：`snapshot_id` 已存在且 manifest 字节 SHA 相同 →
  直接返回库内原报告文本（字节一致）；同 ID 不同 manifest 字节 → `ConflictError`。
* 提交顺序：先 `COMMIT` 持久化快照与封存修订集合，再用临时文件 + `os.replace`
  原子发布外部 `REPORT.json`。崩溃在提交前 → 无任何痕迹，重试即可；
  崩溃在提交后、文件发布前 → 重放同一 manifest 会取回库内原报告并重新发布，
  结果不变。外部输出路径是符号链接或与 manifest 同路径时拒绝。
* 任何无效请求在事务内抛异常 → 回滚，账本、封存记录与旧报告均不变。

## 4. 封存口径与报告结构

* 两侧稀疏合并结果只保存出现过的类别，绝不按"类别域 × 修订 × 分区"物化零值；
  投影到声明域时缺失类别按 0 处理（与 `analyze` 的缺行语义一致）。
* 平滑沿用全域加 alpha 语义：`p=(r_c+alpha)/(N_r+alpha*K)`、
  `q=(n_c+alpha)/(N_n+alpha*K)`，PSI 与自然对数 JS 完全复用 `divergence.compare`，
  固定示例清单的数值与 `analyze` 逐项相同（见测试 `FixedFixture`）。

报告字段：

```
schema_version, snapshot_id, manifest_sha256, alpha, categories,
reference/current: { total, partitions: [ {partition, tip_revision,
                       chain: [{revision, sha256}, ...]} ] },
merged_reference_counts, merged_current_counts,   # 稀疏，仅非零类别
result: analyze 同构输出（精确 p/q、差值、PSI、JS、appeared/disappeared）
```

## 5. CLI

```sh
python -m distribution_drift.snapshot_cli init   --db DB
python -m distribution_drift.snapshot_cli ingest --db DB --source SHARD.json
python -m distribution_drift.snapshot_cli seal   --db DB --manifest MANIFEST.json --output REPORT.json
```

* `ingest` 输出 `{"status": "ingested|idempotent", "content_sha256": ...}`；
* `seal` 首次输出 `sealed`，同 ID 同 manifest 字节重放输出 `replayed` 且报告字节不变；
* 任何契约/冲突/封存拒绝以退出码 2 与 stderr 消息返回，不产生部分写入。

## 6. 限额汇总

| 对象 | 上限 |
|---|---|
| 输入 JSON | 8 MB，重复键/非有限数拒绝 |
| 每库 `(side, partition)` 链 | 100 000 |
| 每条链修订 | 1 000 |
| 每修订非零类别 | 10 000 |
| manifest 类别域 | 1..1 000，唯一非空 |
| 每侧合并分区 | 1..1 000，分区引用唯一非空 |
| alpha | 1..1 000 000 严格正整数 |
| delta | 严格整数、非布尔、非零、绝对值 ≤10⁹ |
| 累计/合并每类计数 | 非负且 ≤10⁹，每侧总量 > 0 |

## 7. 固定示例

`fixtures/snapshot/` 下的分片与 `manifest.json` 与 `fixtures/input.json`
使用相同类别域 `[a,b,c]`、alpha=1，两侧总量各 10；封存结果的 PSI/JS、
精确分数与出现/消失类别与 `analyze` 完全一致。该目录均为自制合成数据（MIT）。

## 8. 测试

`tests/test_snapshot.py` 覆盖：账本重放幂等、字节冲突、精确字节身份、
缺父悬空链保留、错误父 SHA 不可封存、迟到父修订一次解锁后代、
负累计链拒绝封存但保留、越域 delta、零总量与 10⁹ 上限、快照身份与重放、
封存后新后代不可改变旧报告、重启持久性、独立 Fraction oracle 随机核对
总量/PSI/JS/出现消失类别（并与 `analyze` 交叉验证）、固定夹具等值、
两个独立连接交错补父与封存、CLI 端到端与失败保留。
