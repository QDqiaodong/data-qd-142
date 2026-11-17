本目录为原创虚构合成数据，使用MIT许可。没有真实个人、线上实验、生产日志、交易、项目进度或搜索用户信息；不来自外部平台。

* `config.json` — 两个采集源（sensor-a、sensor-b）、三个类别、alpha=1、窗口宽度 10。
* `events-1.jsonl` / `events-2.jsonl` — 两个连续 apply 批次；第二批中 sensor-a 的
  seq=6（ts=9）在其水位 18 之后到达，被隔离为 late，不进入任何窗口。
* `expected/window-20.json`、`expected/window-30.json` — 依次 apply 两个批次后 export
  的逐字节期望报告；数值与 `distribution_drift.model.analyze` 完全一致。
