# M9：期限约束因果残差—联合成交收益—分布鲁棒 MPC

> 英文名：M9 Deadline-Constrained Causal Residual, Joint Fill–Return, Distributionally Robust Model Predictive Control（M9-DCR-DRO-MPC）

- 状态：**M9 策略层已实现并接入可选模拟计划；尚未完成校准、外样本验证或任何新增实盘授权**
- 深化规格：[M9_V2_MATHEMATICAL_EXECUTION_SPEC.md](M9_V2_MATHEMATICAL_EXECUTION_SPEC.md)。v2 对会计、可成交价格、安全回退、统计和求解器的修订优先于 v1；版本冻结不等于参数已校准。
- 谱系：M9 是 M8 的严格子策略，继承 M8 的全部不可变合同和资金费控制。
- 目标：在真实 Binance 合约、成交、费用、资金费、延迟、盘口和期限退出约束下，最大化可验证的全生命周期扣费后增长。
- 非目标：不以“产生更多订单”、短窗未实现收益或单次最佳回测为优化目标。

### 当前代码实现边界

- 策略实现：`engine/src/strategy/m9.rs`；当前使用显式版本化的保守 bootstrap prior，尚未用历史成交数据校准。
- 模拟接入：`SimulationPolicyVariant::M9DeadlineCausalDroMpc` 与 `ExperimentPlan::m1_to_m9()`；批量入口默认仍是 M1–M8，只有 `--include-m9` 才启用 M9。
- 当前实现提供确定性有限期限残差、Null-RW 折扣、保守成交下界、退出容量和 maker-only 动作；联合 hazard、完整 DRO 场景库、在线校准和外样本证书仍属于后续验证工作。
- 代码接入或单元测试通过，不等于策略已证明盈利，也不改变任何实盘授权。

## 1. 结论与诚实边界

M9 的核心不是更低阈值，而是只在以下条件同时成立时承担风险：参考价格可识别、残差具有可交易的有限期回复证据、订单的联合成交后收益为正、完整退出计划可行、最坏分布下风险预算仍满足。

任何策略都不可能诚实保证“百分之两百赚钱”，也不能保证在无界价格跳跃、交易所停摆或规则突变下绝对不亏。M9 把高收益视为压力约束内的结果，把生存、可复现和证据质量设为先决条件。200% 只能作为情景分析中的收益刻度，不能成为调参标签、承诺或晋级门槛。

M9 的最强之处应当是：无法识别时不交易；退出不可行时不建仓；模型分歧变大时自动缩仓；实现证据不支持时自动降级，而不是用复杂模型掩盖数据不足。

## 2. M9 相对 M8 的唯一新增能力

M8 已有资金费感知、稳健半径、CVaR、库存控制和候选报价。M9 只新增四项可单独消融的能力：

1. **统一因果残差**：入场、持仓、退出和归因使用同一版本化公平价值定义。
2. **联合成交—收益—退出模型**：不再用成交概率乘无条件 alpha。
3. **期限约束控制**：在建仓时同步求解退出路径，不把平仓留到 shutdown。
4. **分布鲁棒安全优化**：在模型、队列、延迟和跳跃不确定集合上求安全可行解。

## 3. 不可变核心与可变模型

| 层 | 不可变核心（运行内冻结） | 可变但必须版本化的模型 |
|---|---|---|
| 时间与数据 | 事件时间、接收时间、序列、重放顺序、断流语义 | 状态分类、特征窗口 |
| 锚点 | 官方收盘事件、交易日、币种、复权与 PriceMode 合同 | 锚点置信度、跨市场修正 |
| 行情真相 | Binance 快照/增量连续性、公开成交、mark/index 原始值 | 自身订单剔除盘口、冲击估计 |
| 合约规则 | tick、step、notional、margin、funding、quanto/USDT 变换 | 规则不确定半径 |
| 执行 | 订单状态机、延迟时间戳、拒单、部分成交、撤单竞态 | 队列后验、成交 hazard |
| 会计 | cash、position、fees、funding、保证金、结算和守恒 | 风险预测，不得改写会计 |
| 策略边界 | 只读 DecisionContext，只输出 OrderIntent/ExitPlan | 公平价值、残差、动作评分 |
| 证据 | 运行、数据、代码、参数 digest；训练/验证/锁箱边界 | 在线后验，仅使用当时可见数据 |

不可变的是核心语义和历史记录，不是永远沿用启动时的交易所规则值。规则变化以带 valid-time/known-at 的版本事件进入核心并触发模型失效/切换；未知新规则下停止新增风险。策略不得直接生成成交、写余额、写仓位、修改规则真相或读取未来事件。

正常 M9 严格 maker-only。任何主动减仓只能属于独立、预注册、人工批准的 EmergencyExecutionPolicy；它不是 M9 的隐藏分支，也不得在回测中偷偷启用。

## 4. 可交易对象和价格模式合同

每个 instrument-session 必须固定 AnchorContract：

$$
A_i^{contract}=T_i\!\left(A_i^{native},FX_i,m_i,adj_i,PriceMode_i\right)
$$

其中 T_i 是版本化的可审计变换。USDT 报价、港股本币报价后 USDT 结算、Quanto 合约不得共享未经证明的单位换算。PriceMode 至少区分正常外部指数、闭市 Orderbook-EWMA、维护/退化模式。

闭市 Orderbook-EWMA index、mark、mid 和盘口不是独立外部证据。若它们由同一 Binance 盘口派生，模型必须标记共同因子，禁止把相关输入当作四票独立确认。

## 5. 信息集、状态与无前视

决策时信息集记为 $\mathcal F_t$。状态只包含 t 时刻已经可见并通过质量门禁的数据：

$$
S_t=(A,T_i,PriceMode,L_t^{-self},Trades_t,Index_t,Mark_t,Funding_t,Account_t,Clock_t,Health_t).
$$

$L_t^{-self}$ 是剔除自身挂单影响后的盘口视图。训练标签可以在事后生成，但在线决策只能加载在本次运行开始前冻结的模型，或使用严格 prequential 的过去数据更新。任何跨折标准化、全样本状态分类和未来成交标签泄漏都属于前视错误。

本地订单簿必须验证 Binance update ID 连续性；断裂后先失效盘口，再取新快照重建。在重建完成前，M9 只能 NO_ACTION 或 reduce-risk，不能沿用陈旧队列后验。

## 6. 统一潜在公平价值与残差

令 $a_t=\log A_t^{contract}$，$p_t=\log P_t^{ref}$ 为统一对数参考价格。mid/microprice 都只是估计特征，不保证可成交；收益必须用逐笔成交价和数量相关的 bid/ask VWAP 计算。分解：

$$
p_t-a_t=n_t+u_t,\qquad n_t=f_t-a_t,\qquad u_t=p_t-f_t.
$$

$n_t$ 是锚点后信息创新，$u_t$ 才是候选暂时错价。所有入场、退出、阈值、PnL 归因必须引用同一个 FairValueVersion；禁止先用静态锚点触发，再用动态公平价值解释。

采用状态切换厚尾跳跃模型：

$$
df_t=b_{r_t}^{\top}dZ_t+\sigma^f_{r_t}dW_t+dJ^f_t,
$$
$$
du_t=-\kappa_{r_t}(u_t)dt+\sigma^u_{r_t}dB_t+dJ^u_t.
$$

状态 $r_t$ 至少包括 Reversion、Drift、Jump、LiquidityVacuum、Transition。创新使用 Student-t 或经验厚尾分布；跳跃单独建模。模型集中必须永久包含“无回复随机游走”对手模型，防止系统把所有偏离解释成 alpha。

### 6.1 因果正交化

候选特征先解释可观测的共同市场、FX、行业、资金费和盘口共同因子，再对剩余残差建模。离线估计采用按交易日 purged/embargoed cross-fitting；在线 nuisance 参数只能用过去样本更新。

使用正交得分降低第一阶段公平价值误差对交易效应估计的敏感度，但不把“因果”一词当作保证：只有在预注册识别假设、安慰剂检验和外样本稳定性通过后，才能把残差回复称为可交易效应。

### 6.2 模型集合与后验

初始实现不在热路径使用不可解释的大模型。模型集合至少包含：

- Null-RW：无回复随机游走；
- Robust-OU：带死区和 Student-t 创新的状态 OU；
- Monotone-Empirical：按残差、波动、时段的单调经验转移；
- Change-Point：突变后快速提高无回复/跳跃权重。

权重按逐事件预测对数得分更新，并设置 Null-RW 最低权重。检测到结构突变时扩大不确定集合、缩短记忆、冻结增仓，而不是立刻用少量新样本追涨杀跌。

## 7. 联合成交—收益—退出分布

动作不是一个价格，而是：

$$
a=(side,price,size,TTL,persistence,entry\_cancel\_rule,ExitPlan).
$$

对每个动作直接预测联合分布：

$$
P(V_f,T_f,M_{1s},M_{5s},M_{30s},T_x,V_x,Q_T\mid\mathcal F_t,a).
$$

其中 $V_f/T_f$ 为成交量/成交时刻，$M_h$ 为成交后可执行 markout，$T_x/V_x$ 为退出时间/退出量，$Q_T$ 为期限末残余库存。成交、逆向选择和退出能力必须联合生成，不能假设条件独立。

### 7.1 竞争风险与队列后验

订单状态采用离散时间竞争风险：未成交、部分成交、完全成交、主动撤单、交易所撤单、拒单、报价失效、期限到达。hazard 依赖：队列区间、订单流失衡、成交强度、撤单强度、价格移动、延迟、状态和自身订单年龄。

初始 queue-ahead 不是零常数。它是基于下单确认时可见数量、不可见撤单分配规则和时间优先级的区间后验。撤单/改单必须付出丢失队列优先级和撤单竞态成本。

模型至少校准 $P(fill\le 1s,5s,30s,5m)$，以及条件于成交的可执行 $M_{1s},M_{5s},M_{30s}$。评价重点是成交后的净 markout，而不是孤立 fill rate。

### 7.2 三世界隔离

| 世界 | 用途 | 允许结论 |
|---|---|---|
| Factual replay | 重放真实可见事件 | 订单在既定 tape 上是否可能成交 |
| Structural queue | 校准队列区间和撤单分配 | 成交概率敏感度 |
| Generative stress | 生成延迟、跳跃、断流、深度崩塌 | 风险边界与反向压力 |

三类结果不得混成一个收益数字。历史重放无法知道自身未发生订单会怎样改变未来市场，因此大单影响必须在生成式压力世界中计入，不能由 replay 假装解决。

## 8. 全生命周期净收益定义

对路径 $\omega$，永续合约收益由合同化会计给出，而非现货全额买卖现金流：

$$
\Pi_{t:T}=\Delta RP+\Delta UP+Funding-Fees+Adjustments,
$$

RP/UP 是已实现/未实现损益，外部出入金从业绩中剔除。线性合约每笔闭仓损益为方向乘闭仓数量乘合约乘数乘退出/入场价差；Quanto 必须调用独立 payoff 合同。未成交的假想退出不能写 RP，退出冲击已进入成交价时不得重复扣除。完整会话净权益路径包含全部未闭仓/失败退出；闭环 realized PnL 单独报告，不能只筛选成功闭环样本。详见 v2 §3。

strategy_pnl 必须明确定义为相对冻结基准的执行增益或残差捕获，不能把市场方向收益、未实现收益或 mark-to-fill 差额重命名为 alpha。

## 9. 候选动作与期限控制

每次决策生成有限、确定性的候选集：NO_ACTION、KEEP、CANCEL、各合法 tick 的 maker quote、离散仓位尺寸、不同 TTL 和 ExitPlan。NO_ACTION 必须始终存在，但只有在空仓、无未确认风险订单时才可作为安全默认；已有库存或挂单时调用独立 SafetySupervisor，执行已授权撤单、maker 减仓与对账，无法执行则显式记录风险。

每个新仓动作必须同时创建退出计划：最迟开始退出时刻、目标库存轨迹、可接受队列等待、撤改单规则、资金费边界、开盘/维护前的残余库存上限。没有可行 ExitPlan 的 entry 不是候选动作。

期限 $T_d$ 取外部市场开盘、资金费结算、维护、会话结束或风险窗口中最早者。库存轨迹惩罚随剩余时间非线性上升：

$$
c_{deadline}(q,t)=\lambda_q(t)q^2,\qquad \lambda_q(t)\uparrow\infty\quad\text{as }t\to T_d.
$$

实际实现用有限上界和硬残余仓位约束，避免数值发散。期限分为 Build、Harvest、Exit、Reconcile 四相；进入 Exit 后禁止新增同向风险。

批处理 shutdown 必须先停止新风险，继续保持行情、订单更新和执行通道，直至退出/撤单/对账完成或明确记账为 residual exposure。先关闭市场任务再发 maker 平仓单属于不可成交的伪平仓。

## 10. 分布鲁棒安全优化

不确定集合 $\mathcal U_t$ 同时覆盖：模型后验、Null-RW/跳跃对手、队列区间、延迟分位数、费用/资金费、规则版本误差、盘口深度下降和相关性破裂。

滚动窗口目标为：

$$
\max_{\pi\in\Pi_{safe}}\ \inf_{Q\in\mathcal U_t}
\mathbb E_Q\!\left[\log\frac{W_{T_d}}{W_t}\right]-\eta\,Turnover(\pi)-\xi\,Uncertainty(\pi)
$$

满足：

$$
CVaR_{\alpha,Q}(L_{T_d})\le B_t,\quad
\Pr_Q(DD_{T_d}>D_t)\le\varepsilon_t\quad\forall Q\in\mathcal U_t,
$$
$$
MarginBuffer_s\ge M_{min},\quad |Q_{T_d,s}|\le q^{res}_{max,s},
$$

并满足总敞口、标的集中度、共同因子 beta、资金费、退出容量和价格冲击约束。风险约束使用同一损失定义，避免在目标和多个惩罚项中重复计算同一尾部风险。

### 10.1 字典序求解

M9 不把所有目标粗暴压成一个可被权重钻空子的分数，按以下层级求解：

1. L0 数据、规则、单位、会计和模型合同有效；
2. L1 所有规定压力场景内存在安全退出路径；
3. L2 净期望收益下置信界超过费用、模型误差和机会成本门槛；
4. L3 在 L0–L2 可行集中最大化最坏情形期望对数增长；
5. L4 同值时选择换手更低、订单更持久、解释更简单的动作。

完整 POMDP 不宣称可全局精确求解。初版对有限反馈策略枚举，再以有限场景概率多面体上的 LP 计算鲁棒价值/CVaR；连续对数增长涉及指数锥，不能笼统声称 MISOCP 精确求解。二次近似须给误差界。每次输出候选域、价值下界、可用上界、耗时和条件性 RobustRiskCertificate；无上界时 gap 标 unavailable。

超时、数值异常或无可行解时不得返回未经认证的“近似最优”订单，只能复用仍有效的安全 incumbent、KEEP、CANCEL、NO_ACTION 或 reduce-risk。

## 11. 仓位尺度

先计算证据收缩后的风险约束 Kelly 建议，再取所有硬上限的最小值：

$$
q^*=\operatorname{sign}(e)\min(q_{RCK},q_{CVaR},q_{margin},q_{exit},q_{impact},q_{concentration},q_{ops}).
$$

其中 $q_{RCK}=\rho_{evidence}\rho_{regime}q_{Kelly}$，且 $0\le\rho\le1$。证据少、模型分歧大、分布漂移或实盘校准差会把尺寸连续压向零。禁止因“近期没交易”提高风险，也禁止用亏损加仓恢复目标收益率。

组合层必须对共享锚点、相同底层股票、币种、时段和 Binance 盘口因子做聚类暴露约束；七个标的不是七个独立赌注。

## 12. 黑天鹅生存合同

压力集至少覆盖：

- 盘口深度瞬间减少 50%/80%/95%，价差扩大，撤单激增；
- 价格厚尾跳跃、连续跳跃、开盘缺口和残差不回复；
- WebSocket 断流、update ID 断裂、订单回报乱序；
- 下单/撤单延迟达到历史 p99/p99.9 或成倍增加；
- 部分成交后市场向不利方向移动，另一腿无法退出；
- funding 符号/幅度突变，mark/index/LOB 相关结构改变；
- 拒单、规则变化、精度变化、维护和账户状态不确定；
- 多标的相关性在压力期趋近 1。

离线求近似 robust viability kernel：只有从当前账户状态出发，在规定压力集下仍能通过合法动作保持保证金、敞口和期限库存约束的状态才允许建仓。每次版本升级还要做 reverse stress：求使风险约束首次失效的最小深度、延迟、跳跃和断流组合。

“生存”是对明确威胁集和资本预算的条件性陈述，不是对无限损失的保证。绝对 maker-only 意味着必须接受某些场景不能及时退出，因此 q_exit 和 q_margin 必须按残余仓位全额压力损失定尺。

## 13. 在线学习、漂移与降级

在线只做逐事件预测—观察—评分更新，不接触锁箱结论。记录 PIT/可靠性、Brier/log score、成交 hazard 校准、markout 校准、退出完成率和残差覆盖率。

使用时间一致的置信序列监控关键均值/概率，避免反复查看普通 p 值造成可选停止偏差。Page-Hinkley/CUSUM 或贝叶斯变点只负责扩大不确定性和触发降级，不能自动放宽交易门槛。

降级顺序固定：M9 Full → M9 Null-heavy → M8/M7 已验证安全子集 → NO_ACTION/ReduceOnly。任何升级都需要新的版本、外样本证据和人工批准；降级可自动发生。

## 14. 类型化接口与模块边界

| 类型 | 必需字段 | 责任 |
|---|---|---|
| AnchorContract | instrument、session、PriceMode、T、digest | 单位与锚点真相 |
| M9DecisionContext | as_of、LOB-self、trades、account、health | 只读输入 |
| M9BeliefState | regime posterior、FV posterior、residual、uncertainty | 可变模型状态 |
| JointOutcomeForecast | fill/markout/exit joint scenarios、calibration id | 联合预测 |
| CandidateAction | quote、size、TTL、persistence、ExitPlan | 有限动作 |
| RobustRiskCertificate | ambiguity id、constraints、worst case、gap | 安全证明 |
| M9Decision | fair value、residual、EV-LCB、target、intent、reasons | 唯一策略输出 |

建议新增但本轮不实现的模块：

- engine/src/strategy/m9.rs：候选生成和决策编排；
- engine/src/strategy/m9/belief.rs：状态后验与变点；
- engine/src/strategy/m9/joint_outcome.rs：联合成交/markout/退出；
- engine/src/strategy/m9/optimizer.rs：鲁棒有限视界优化；
- engine/src/strategy/m9/certificate.rs：约束证书和降级；
- engine/src/strategy/m9/contracts.rs：类型与版本 digest。

模拟器、会计、市场真相和订单状态机不得放进 m9 模块。method_graph 注册 M9 时必须声明 parent=M8，并把联合结果、期限退出和鲁棒优化列为独立方法节点，保证可做消融。

## 15. 热路径预算与确定性

所有候选价格先通过交易所精度和 maker-only 过滤；场景树离线/低频更新，热路径只做有限候选重评分。每个决策设置硬时间预算，超过预算执行认证回退。

同一 event tape、模型快照、参数 digest 和随机种子必须得到逐事件相同决策。浮点比较、排序 tie-break、场景顺序和 solver tolerance 全部写入 manifest。

### 15.1 强制校准包

M9 不允许埋在代码里的静默参数。每个 instrument-session 必须加载带有效期和训练截止时间的 CalibrationBundle，至少包含：

- 候选 tick 距离、size 网格、TTL、决策频率和 solver 预算；
- fill/partial/cancel hazard 与 queue-ahead 区间；
- 1s/5s/30s/5m 条件 markout 和 exit-time 分布；
- regime 转移、Null-RW 下限权重、跳跃/厚尾参数；
- fee、funding、latency、impact、depth stress 和规则 digest；
- NAV 风险预算、CVaR 水平、回撤/保证金/集中度/期限库存上限；
- 训练区间、purge/embargo、校准误差、适用状态和过期条件。

数值不得从本设计文档硬编码。缺少、过期或越出适用域时输出 calibration_unavailable 并 NO_ACTION。首个 CalibrationBundle 由 P1–P3 的真实数据产生，接受审计后冻结；后续版本不得覆盖历史包。

## 16. 实验矩阵

| ID | 相对 M8 唯一变化 | 要回答的问题 |
|---|---|---|
| M9-A0 | 原样 M8 | 基线 |
| M9-A1 | 统一 FairValueVersion | 混合参考是否制造伪信号 |
| M9-A2 | 有限期残差模型 | 回复是否在退出期限内发生 |
| M9-A3 | 加 Null-RW/Jump 对手 | 收益是否依赖必然回复假设 |
| M9-A4 | 联合 fill-markout | 独立模型是否高估 EV |
| M9-A5 | 队列持久性与撤单竞态 | 频繁改单是否破坏收益 |
| M9-A6 | entry 同时求 ExitPlan | 完整闭环率是否提高 |
| M9-A7 | DRO + 风险约束 Kelly | 收缩后收益/尾部是否更好 |
| M9-A8 | 组合相关暴露 | 多标的是否只是重复 beta |
| M9-A9 | 在线校准与降级 | 漂移时是否先缩仓 |
| M9-Full | A1–A9 | 交互后是否仍优于 M8 |

每个消融使用同一不可变 event tape 和独立策略状态；不得复制完全相同的 R 结果充当独立样本。训练/验证/锁箱按交易日和市场状态分块，参数搜索记录完整试验族。

现实扰动按延迟、深度、队列、费用、funding、跳跃、断流、拒单、开盘缺口和部分平仓做笛卡尔子集及定向极值搜索。Factual、Structural、Generative 三世界分别报告。

## 17. 指标合同

每次运行至少输出：

- 完整闭环 realized PnL、residual liquidation value、fees、funding；
- market PnL、execution PnL、residual-capture PnL，定义和恒等式可对账；
- fill/partial-fill hazard、条件 markout、queue error、order lifetime；
- ExitPlan 完成率、期限残余库存、撤单竞态和 reconciliation；
- max drawdown、CVaR、expected shortfall、margin buffer、reverse-stress distance；
- 模型 PIT/Brier/log score、覆盖率、分歧、变点和降级次数；
- solver status、耗时、gap、certificate、NO_ACTION 原因；
- 数据、代码、规则、模型、参数和 build identity digest。

## 18. 统计检验与晋级门

收益比较单位是独立交易日/闭市会话和完整 closure episode，不是事件条数。使用 paired block bootstrap、Newey–West 或状态分块稳健误差；多策略选择后使用 White Reality Check/Hansen SPA，并报告 Deflated Sharpe Ratio。

M9 不直接最大化样本 Sharpe：分母可被低交易频率、平滑估值和未实现收益操纵。在线目标使用扣费后最坏期望对数增长与硬尾部约束；离线只在 X0–X5 全通过的 Pareto 可行集中比较年化净收益、DSR、Sortino、Calmar、回撤和资金利用率的外样本下置信界。不得用某一指标改善交换会计、退出或生存失败。

晋级必须依次通过：

| Gate | 硬条件 |
|---|---|
| X0 构建 | build identity 非 unknown；GNU 工具链和 digest 可复现 |
| X1 真相 | 深度连续、规则版本、单位、会计和 reconciliation 全通过 |
| X2 闭环 | 无意外工作单；残余仓位被显式清算/计价；不以未实现收益晋级 |
| X3 校准 | fill、markout、exit 的可靠性在预注册容差内 |
| X4 经济性 | 扣费/资金费/冲击后，M9 对 M8 的配对净增益 LCB > 0 |
| X5 尾部 | 压力 CVaR、回撤、margin、exit 和 reverse stress 全满足预算 |
| X6 统计 | 多日多状态，Reality Check/SPA/DSR 后仍有证据，且非单标的贡献 |

置信序列只在其假设有效的过程上使用；SPA/DSR 不能把坏数据变成好证据。任何 X0–X3 失败都应先修真实性，不得继续优化收益。

晋级资金采用小步、可逆、预注册阶梯；每一级重新校准模拟与 shadow/live 的成交差异。达到收益目标但校准或尾部失败，结论仍是拒绝晋级。

## 19. 当前 r7/r8 证据如何进入 M9

r7 约 60 分钟、52 单、4 成交，且结束时仍有仓位和工作单，只证明生命周期部分链路可运行。它不能校准联合成交/markout/exit，更不能证明 anchor alpha。

旧报告 net_pnl_ticks=5,704,463，capital_usdt_ticks=140,000,000,000，而报告显示资本为 1400.00000000 USDT。若两者遵守同一 1e8 金额刻度，则净 PnL 约为 **0.05704463 USDT**，不是 5.70 USDT；在会计单位合同独立复核前不得据此计算收益率。

该运行 queue_ahead=0、延迟为零、build identity unknown，且 flat_at_end=false，因此只能作为诊断样本，不能进入 M9 的收益先验或晋级统计。

r8 的 threshold_unavailable/signal_below_threshold 是诊断信息，不是“市场无机会”的证据。M9 必须把 warming_up、insufficient_data、invalid_input、model_failure、unidentifiable 和 economically_negative 分开，并把 reason vector 写入每次决策。

## 20. 实施顺序

### P0：冻结合同和证据

- 锁定 AnchorContract、PriceMode、货币/Quanto 单位和完整 PnL 恒等式；
- 修正 build identity，验证 r7 金额刻度；
- 修复 shutdown 顺序，保证退出期间行情和执行仍在线；
- 把 threshold 不可用原因类型化。

### P1：市场真相与执行校准

- 持久化深度增量、快照、aggTrade、订单 ACK/FILL/CANCEL 和所有时钟；
- 校验 update ID 连续性，断裂强制 resync；
- 建立非零 queue-ahead 区间、trade-through 和撤单竞态；
- 记录网络/策略/交易所各段延迟分布。

### P2：统一参考与残差

- 统一静态触发和动态 fair value；
- 实现 Null-RW、Robust-OU、经验模型和状态后验；
- 先做离线 prequential 评估，再允许在线只读加载。

### P3：联合结果模型

- 生成 fill/partial/cancel/markout/exit 的共同标签；
- 做概率校准、条件 markout 和持有期分层；
- 用结构化队列与生成压力验证 replay 偏差。

### P4：期限退出

- Entry 强制携带 ExitPlan；
- 实现 Build/Harvest/Exit/Reconcile 状态机；
- 对 opening、funding、maintenance 做随机终端情景。

### P5：鲁棒优化与证书

- 建有限动作和场景集；
- 实现字典序 LP/SOCP/MISOCP 与超时回退；
- 输出逐约束最坏场景、slack、gap 和拒绝原因。

### P6：组合层

- 实现风险约束 Kelly 收缩和多上限取最小；
- 加共同因子、集中度、退出容量和影响约束；
- 验证单标的故障不会污染组合会计。

### P7：消融与统计

- 执行 M9-A0 至 M9-Full；
- 多日、多状态、锁箱、Reality Check/SPA/DSR；
- 只有通过 X0–X6 才进入 shadow。

### P8：shadow 与小资金

- 对比模拟与 shadow/live 的 fill、markout、latency 和 exit；
- 误差超界自动退回 NO_ACTION；
- 资金逐级、可逆晋级，不设置追赶收益目标。

## 21. 明确拒绝的捷径

- 不通过降低阈值解决长期无交易；
- 不用 index/mark/mid/LOB 的相关投票伪造置信度；
- 不把 fill probability 与无条件 alpha 相乘；
- 不把 queue_ahead=0 或零延迟当现实；
- 不用 mid 估值掩盖期限残余库存；
- 不将生成式压力收益与 factual replay 收益合并；
- 不让在线模型读取锁箱结果后自我调参；
- 不以 Sharpe 单指标覆盖尾部、校准和对账失败；
- 不承诺固定收益率，不以加杠杆追赶 200%。

## 22. 完成定义

M9 只有在代码、测试、数据、校准和 X0–X6 证据全部完成后，才能从“设计冻结”改为“已验证”。仅写出数学公式、跑通编译或在一段行情中盈利都不算完成。

最终验收不是“模型最复杂”，而是：同一输入可重放、每个动作可解释、每笔现金可对账、每个风险约束有证书、每项超额收益有外样本证据、每次模型失效都有安全降级。

## 23. 研究与工程依据

以下资料只支持对应局部方法，不直接证明 AnchorBell 或 M9 盈利：

1. Binance，How to Manage a Local Order Book Correctly：快照/增量衔接、pu/u 连续性和绝对数量语义。
   https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
2. Binance，TradFi Perpetual Contracts Index Price Calculation Update（2026-05-16 生效）：闭市/维护阶段 Orderbook EWMA 模式。
   https://www.binance.com/en/support/announcement/detail/53bfc17634f54f2f90666dbc396f5cee
3. Binance，Quanto Contracts：本币报价、USDT 结算的合约语义。
   https://www.binance.com/en/support/announcement/detail/18724cba64a048938986a98bbb257258
4. Barzykin, Bergault, Guéant & Lemmel, Optimal Quoting under Adverse Selection and Price Reading：信息风险、库存与报价控制。
   https://arxiv.org/html/2508.20225v1
5. Bodor & Carlier, MDQR：跨档位依赖、订单尺寸和深度队列反应建模。
   https://arxiv.org/html/2501.08822v1
6. Market Simulation under Adverse Selection：价格与订单流独立模拟会夸大短期表现。
   https://arxiv.org/html/2409.12721v1
7. HftBacktest，Order Fill：历史 replay 无法模拟自身市场冲击，队列位置需要单独建模。
   https://hftbacktest.readthedocs.io/en/py-v2.1.0/order_fill.html
8. Busseti, Ryu & Boyd，Risk-Constrained Kelly Gambling：带回撤概率约束的增长优化。
   https://stanford.edu/~boyd/papers/kelly.html
9. Sun & Boyd，Distributionally Robust Kelly Gambling：不确定分布集合下的最坏期望对数增长。
   https://web.stanford.edu/~boyd/papers/robust_kelly.html

10. Rockafellar & Uryasev，Optimization of Conditional Value-at-Risk：CVaR 的可计算优化表达。
    https://uryasev.github.io/publications/
11. Bailey & López de Prado，The Deflated Sharpe Ratio：选择偏差、非正态和多重试验修正。
    https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
12. Howard et al.，Time-uniform, nonparametric, nonasymptotic confidence sequences：任意停止下的时间一致推断。
    https://arxiv.org/html/1810.08240v9
13. He, Manela, Ross & von Wachter，Fundamentals of Perpetual Futures：永续合约、资金费与非必然收敛边界。
    https://arxiv.org/abs/2212.06888

## 24. 最终决策规则

对每个候选动作，M9 依次回答：

1. 数据、单位、规则、时钟、盘口、账户和模型版本是否有效？
2. 残差相对 Null-RW/Jump 对手是否仍有有限期限内的回复证据？
3. 条件于真实队列和延迟，联合成交后净收益下界是否为正？
4. 从建仓到期限，是否存在经压力验证的完整退出路径？
5. 在全部不确定分布内，CVaR、回撤、margin、集中度和残余库存是否合格？
6. 求解器是否在预算内给出可复现的安全证书？

任一答案为否，拒绝新增风险并由 SafetySupervisor 处理既有库存/订单。全部为是，在可行候选中选择鲁棒目标最大的 maker intent；仅在目标值处于数值等价容差内时优先较低风险、较小尺寸，而非一律选最小单。

这就是 M9 的完整原则：**先证明这笔交易在真实生命周期中值得存在，再讨论它能赚多少。**
