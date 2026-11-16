# 实体人群迁移对账管道（audit / reconcile）

既有 `analyze` 只能比较两期聚合边际频数：即使 PSI=0，也无法区分"同一批实体保持
原类"与"实体之间互换类别"。本管道在实体粒度上读取 reference/current 两份观察明细，
经候选冲突审计与人工裁决，输出留存（retained）、流入（entered）、流出（exited）
与类别迁移（changed），并把接纳后的边际交回现有 `analyze`。仅用 Python 标准库与
SQLite 外存；不新增分区修订、封存或快照数据库。

## 1. 数据契约

### 1.1 观察明细

UTF-8 JSONL，每行一个严格对象：

```json
{"entity": "e-001", "category": "a"}
```

* 只有 `entity`、`category` 两个字段，未知字段拒绝；`entity` 沿用 `common.ident`
  （非空、无首尾空白、≤128 字符），`category` 必须属于配置声明域。
* 拒绝空行/纯空白行、重复 JSON 键、`NaN`/`Infinity` 等非有限常量、非 UTF-8 字节、
  单行超过 65536 字节（含换行）。文件允许为空。
* 单侧最多 10 000 000 行，行序任意；跨文件以 entity 字符串精确相等匹配，
  不做大小写折叠。
* 同一侧同一实体同一类别的重复观察**折叠为一个候选**，但保留原始行数 `rows`；
  同一侧同一实体出现多个**不同类别**才构成冲突。冲突不允许最后一行覆盖，
  也不按频次投票。

### 1.2 配置

严格 JSON `{"categories", "alpha"}`，类别域（1..1000 个唯一 ident）与
alpha（1..10⁶ 严格正整数）完全沿用 `model.analyze` 契约。

### 1.3 裁决（仅 reconcile）

UTF-8 JSONL，每行：

```json
{"side": "reference", "entity": "e-001",
 "expected_categories": ["a", "b"], "selected": "a"}
```

`expected_categories` 是排序且唯一的**完整**候选类别列表，`selected` 是该集合中的
一个类别，或 `null`（明确排除）。裁决只对当次该侧的冲突实体生效，下列情况整体报错、
不悄悄忽略陈旧裁决：重复裁决、目标实体不存在、目标实体不是冲突（仅一个候选）、
候选集合与审计结果不精确匹配（含顺序/越域）、选中值不属于候选集合。
没有裁决的冲突保持未解决。

## 2. 外存结构与处理阶段

所有与实体数/行数成正比的状态都在**每次调用新建、结束即删除**的临时 SQLite 库中
（`CohortStore`），Python 侧只保留标量计数、有界类别域与单行游标；任何扫描都不使用
`fetchall`，也从不物化类别域笛卡尔积（只存非零格）。

| 表 | 键 / 形态 | 作用 |
|---|---|---|
| `obs(side, entity, category, rows)` | `WITHOUT ROWID` 聚簇表，主键 `(side,entity,category)` | 折叠后的候选；`rows` 是该候选的原始行数 |
| `ruling(side, entity, selected)` | `WITHOUT ROWID`，主键 `(side,entity)` | 裁决外存；`selected` 可空 |
| `entity_union(entity)` | 两侧实体并集 | 迁移全外连接的驱动集合 |
| `side_entity(side, entity, ncat, has_ruling, selected)` | 每侧每实体一行 | 资格裁定的完整候选视图 |
| `eligible(entity)` | 通过双侧资格裁定的实体 | 隔离闸门 |
| `effective(side, entity, category)` | 每侧每实体至多一个生效类别 | 迁移连接输入 |
| `cell(refcat, curcat, n)` | 普通 rowid 表（允许 NULL 键） | 非零迁移格 |

### 2.1 摄取与折叠

明细按一次性顺序迭代器消费（支持任何 yield bytes 的不可 seek 生成器）；文件对象按
8 KiB 小块读取并在切块时执行 65536 字节行上限，超长行不可能撑爆 Python 缓冲。
每 10 000 行用 UPSERT 批量落盘：

```sql
INSERT INTO obs(side, entity, category, rows) VALUES (?, ?, ?, 1)
ON CONFLICT(side, entity, category) DO UPDATE SET rows = rows + 1
```

因此重复观察的折叠在入库时完成，`rows` 始终按实计量；任何解析/限额错误都在事务内
回滚，临时库不留半成品。

### 2.2 候选审计

冲突 = 同侧同实体 `COUNT(DISTINCT category) > 1`。`iter_conflicts` 以
`ORDER BY side, entity, category` 的流式游标输出，逐实体拼装
`{side, entity, candidates:[{category, rows}, ...]}`，只输出冲突实体；
平票与不平票冲突在结构上完全同构，不存在投票逻辑。

### 2.3 裁决资格：先看两侧全部候选，再决定接纳

裁决入库时逐条对 `obs` 做有界（≤类别域大小）候选核对，错误立即整体失败。资格裁定
在全部摄取完成后由集合运算一次完成，绝不会看到唯一候选就提前计入统计：

```sql
INSERT INTO eligible
SELECT u.entity FROM entity_union u
WHERE NOT EXISTS(
  SELECT 1 FROM side_entity s
  WHERE s.entity = u.entity
    AND s.ncat > 1 AND (s.has_ruling = 0 OR s.selected IS NULL));
```

只要实体在**任一侧**有未解决冲突（无裁决）或明确排除（`selected IS NULL`），
该实体就在两侧整体隔离：另一侧的唯一观察也不会变成虚假流入/流出。资格集合用
`EXCEPT` 集合差一次性算出（而非相关 `NOT EXISTS` 子查询）：`side_entity` 的
`WITHOUT ROWID` 主键以 `side` 为前导列，逐实体的相关子查询无法走索引会退化为
O(n²) 扫描，集合差则是单次排序流运算。`excluded_entities` 是两种原因的并集
（GROUP BY 去重），`unresolved_entities` 与 `explicitly_excluded_entities` 可交叠。
已解决冲突只贡献被选中的一个类别与一个实体，原始重复行数不作为权重。

### 2.4 迁移聚合：接纳集合上的全外连接

```sql
INSERT INTO cell
SELECT e0.category, e1.category, COUNT(*)
FROM eligible g
LEFT JOIN effective e0 ON e0.entity = g.entity AND e0.side = 'reference'
LEFT JOIN effective e1 ON e1.entity = g.entity AND e1.side = 'current'
WHERE e0.entity IS NOT NULL OR e1.entity IS NOT NULL
GROUP BY e0.category, e1.category;
```

`cell` 故意是普通 rowid 表：单侧格的类别列为 SQL NULL，而 `WITHOUT ROWID` 的 NOT NULL
主键会拒绝 NULL。连接按实体列驱动，而 `effective` 主键以 side 为前导列，因此另建
`effective(entity)` 前导索引保证全外连接为 O(n log n)。NULL 只表示"该侧无观察"，
与类别字符串永不混淆，也不可能出现 NULL→NULL 格（`WHERE` 子句保证）。口径：

* 两侧都有观察 → retained；其中 `refcat != curcat` → changed（retained 的子集）；
* 仅 current 有 → entered（`refcat IS NULL`）；
* 仅 reference 有 → exited（`curcat IS NULL`）。

### 2.5 边际守恒与 distribution

行边际（reference）与列边际（current）由 cell 直接 `GROUP BY` 求和，结构上保证：

```
reference_total = retained + exited
current_total   = retained + entered
```

接纳后的两组稀疏边际连同**完整声明域**与同一 alpha 交给现有 `analyze`，
双零类继续按原契约参与平滑。任一侧接纳总量为 0 时不塞伪计数：
`distribution=null`、`distribution_status="insufficient_population"`，迁移与隔离审计
照常输出（`analyze` 本身拒绝零总量快照）。

## 3. 确定性与输出

* 明细重排、重复同类观察不改变迁移与 distribution：折叠与连接均以实体/类别键的
  集合运算为准，与到达顺序无关；冲突 JSONL 与报告内数组均按 Unicode 顺序排列，
  单元格按 `(refcat, curcat)` 排序（SQLite 中 NULL 先于文本）。
* 报告字段：`schema_version, alpha, categories, audit{reference,current:{raw_rows,
  folded_duplicate_rows, conflict_entities, selected_conflicts}}, movement{retained,
  changed, entered, exited}, cells[{from,to,count}], reference_marginals,
  current_marginals, totals, excluded_entities, unresolved_entities,
  explicitly_excluded_entities, distribution_status, distribution`。
* 结果先写同目录临时文件再 `os.replace` 原子发布；符号链接输出与输入/输出同路径
  拒绝；任何非法行或错误裁决（含文件末尾的截断行）使整个请求失败并保留旧目标文件，
  临时数据库关闭即删除，不构成跨次权威状态。

## 4. 资源策略与代价

* **内存**：Python 容器仅持有标量、有界类别域、单条候选（≤1000）与流式游标；
  SQLite 页缓存由 `PRAGMA cache_size=-8192` 限制在约 8 MiB，关闭 mmap、
  `temp_store=FILE`，排序/分组溢出走磁盘。实测 60 万实体（120 万观察行）子进程
  峰值 RSS 约 36 MiB，远低于 128 MiB 预算（测试 `StreamingMemory` 以
  `RUSAGE_CHILDREN` 持续断言）。
* **I/O**：每条输入行顺序读一次、一次 UPSERT（聚簇主键，重复行就地累加）；
  审计与裁决核对走主键索引；迁移阶段一次排序扫描构建四张派生表，报告全程流式写出，
  实体列表与单元格不经过 Python 全量列表。
* **临时空间**：随输入线性增长。`obs` 每候选一行（最坏约等于原始行数），
  `side_entity` 每侧每实体一行，`eligible/effective` 同量级，`cell` 至多
  K² 个非零格且实际只存出现过的组合，绝不物化类别笛卡尔积。
* 裁决可先于资格裁定全部落临时外存；输入迭代器只消费一次，不依赖 seek 或二次重读。

## 5. CLI

```sh
python -m distribution_drift.cohort_cli audit \
    --reference REF.jsonl --current CUR.jsonl --config CONFIG.json --output CONFLICTS.jsonl
python -m distribution_drift.cohort_cli reconcile \
    --reference REF.jsonl --current CUR.jsonl --config CONFIG.json \
    --rulings RULINGS.jsonl --output REPORT.json
```

`audit` 输出按 side/entity/category Unicode 排序的冲突 JSONL（无冲突时为空文件）；
`reconcile` 输出完整对账报告 JSON。两条命令的 `--help` 明确列出参数、输入格式与
资源策略；契约错误以退出码 2 与 stderr 消息返回，不产生部分写入。原有
`python -m distribution_drift.cli`（analyze）与 `snapshot_cli` 入口保持不变。

## 6. 测试

`tests/test_cohort.py` 与流式驱动 `tests/cohort_stream_driver.py` 覆盖：

* 严格行契约（空行、重复键、非有限常量、未知字段、UTF-8、65536 字节边界、行数上限
  与回滚、一次性非 seek 迭代器）；
* 候选审计的远距离重复折叠、平票/不平票冲突、精确候选列表与 Unicode 排序；
* 陈旧、重复、非冲突/不存在实体、候选集合不匹配、选中域外等裁决拒绝；
* 互换类别但相同边际（PSI=0 但 changed 可识别）、纯流入、纯流出、空输入、
  声明双零类的完整非零格、全部守恒式，以及 distribution 与 `analyze` 逐项一致；
* 联动：reference 的 a/b 冲突 + current 唯一 c，未裁决时两侧都不计入（无虚假
  entered），选择 a 后恰为一个 a→c 的 retained/changed，`selected=null` 继续全局
  排除，以及一侧未解决、另一侧明确排除时隔离集合去重；
* 独立小样本 oracle（40 组随机队列）与打乱输入对照；
* 流式多批次 20 万实体数据的内存测量与临时库清理；CLI 端到端、失败保留与帮助文档。
