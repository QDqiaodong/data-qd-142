# data-qd-142 · 分类分布漂移诊断

Smoothed categorical distribution comparison with coverage audits。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/distribution-drift --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/distribution_drift/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 分区增量账本与封存快照（修订链）

`snapshot.py` + `snapshot_cli.py` 在 SQLite 上提供不可变修订链与可复现封存报告，
设计、事务边界、资源/恢复代价与限额见 [封存快照设计](docs/snapshot.md)。

```sh
python -m distribution_drift.snapshot_cli init --db run/ledger.db
python -m distribution_drift.snapshot_cli ingest --db run/ledger.db --source SHARD.json
python -m distribution_drift.snapshot_cli seal --db run/ledger.db \
  --manifest MANIFEST.json --output run/report.json
```

分片 `parent_sha256` 引用同 side/partition 前一修订**原始输入字节**的 SHA-256；
同版本同字节幂等、同版本异字节冲突拒绝。缺父修订（含迟到更正、负累计链）一律先入账，
封存时在单个 `BEGIN IMMEDIATE` 事务内按需流式追溯完整引用前缀：父链有缺口、
父 SHA 不匹配、delta 越域、任一前缀累计为负、每类计数超 10^9 或侧总量为 0 均拒绝封存；
补齐缺失父修订后同一 manifest 可一次解锁已存后代。封存后报告按 snapshot_id 与
manifest 原始字节 SHA 锁定，重放返回字节一致的原报告，新后代不可改写旧报告。
固定示例在 `fixtures/snapshot/`，其报告数值与 `analyze` 完全一致。

## 当前业务与样例
# Categorical drift accounting v1

供数据监测分析者比较两个聚合频数快照。categories为1..1000个唯一类别，reference/current各最多1000个category/count，唯一且必须在声明域内；缺行表示该声明类别频数0，而非未知漏采。count严格0..10^9整数，每快照总数>0。alpha为1..10^6正整数平滑伪计数。
对所有声明类别做加alpha平滑，p=(reference_count+alpha)/(reference_total+alpha*K)，q同理。输出原频数、精确分数p/q与q-p；PSI=sum((q-p)*ln(q/p))，Jensen-Shannon用自然对数、m=(p+q)/2，JS=(KL(p||m)+KL(q||m))/2。新出现/消失类依据原始0/正频数区分，双0类仍占平滑域。
样例总量各10，平滑a为9/13→3/13、b相反、c为1/13→1/13。相同分布PSI/JS为0。浮点散度非显著性、漂移阈值或根因结论，样本与聚合覆盖应由调用方核实；不处理连续特征分箱或在线告警。

