# AnchorBell 实验系统

## 目标

所有策略方法、消融实验、执行 overlay 和证据要求都必须由同一份实验计划管理。运行时会把解析后的计划写入：

- run-manifest.json：完整运行参数与方法目录。
- experiment-index.json：可被 UI、报表和后处理直接消费的实验索引。
- run-status.json：运行生命周期状态。
- 每个实验目录的 metrics.json、records.jsonl：逐实验结果。

experiment-index.json 中的 experiment_plan_digest 是计划内容的 SHA-256。修改方法、父实验、消融、执行 overlay 或证据策略都会改变摘要，避免结果被错误归因。

## 添加新方法

1. 在 engine/src/strategy/method_catalog.rs 注册方法元数据，声明方法键、父方法、层级、需要的 feature、不可覆盖契约和支持的消融。
2. 在 engine/src/simulation/experiment_plan.rs 中增加对应实验定义，填写唯一 label、strategy、role、parent_experiment_id、execution_overlay 和 evidence_policy。
3. 运行完整检查：cargo fmt --all、cargo check --workspace --locked、cargo test --workspace --locked、cargo clippy --workspace --locked --all-targets -- -D warnings。
4. 只提交包含计划摘要和索引输出的变更；不要手工修改历史运行目录。

## 字段约束

| 字段 | 作用 | 当前允许值 |
|---|---|---|
| role | 区分控制、增量、消融、挑战者和安全 overlay | Control、Incremental、Ablation、Challenger、SafetyOverlay |
| parent_experiment_id | 表示增量/消融来源 | 必须引用前面已声明的实验 |
| execution_overlay | 与策略方法正交的执行层 | maker_only、emergency_reduce_only_taker |
| evidence_policy | 规定结果能否进入下一阶段 | pre_screen_only、oos_required、stress_required、promotion_required |

emergency_reduce_only_taker 只是实验注册项，不等于开启实盘 taker。任何 taker 实现仍必须满足只减仓、IOC/aggressive limit、滑点/数量/cooldown/reconciliation 门禁，并在状态未知时禁止执行。

## 设计规则

- 方法身份与执行 overlay 分离；不要把 taker 变成 M10。
- 共享同一事件带和数据摘要，确保实验之间是配对比较。
- M0 控制、M8/M9 内部组件消融、跨日期/跨标的/OOS/stress 计划应作为显式实验，不应通过修改现有实验含义实现。
- 历史结果只追加，不覆盖；重复回放必须使用新的计划 ID 和运行目录。
