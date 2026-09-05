# AnchorBell 方案持续打磨文档

> 用途：记录 AnchorBell 模拟器与策略的多轮方案审查、决策、实现边界和验证结果。
>
> 规则：后续每一轮继续修改本文件；模拟器与策略必须分开记录；每轮方案先审查、再实现、再跑完整 M1–Mxx 矩阵。

## 0. 文档状态

- 当前轮次：Round 16（M9 设计冻结；尚未实现/验证）
- 日期：2026-09-05
- 当前实验基线：M6 第一版完整矩阵模拟盘（已退出）
- 最近输出目录：target\\simulation-batch-20260904-M6-10000cny
- 最近进程：anchorbell_simulation_batch.exe，PID 23864；2026-09-04 复查时已不存在
- 末条记录为“batch execution environment stopped”，各 ledger 同步收尾；当前记录未保存触发来源/退出码，不能判断是人工信号、父进程结束还是内部退出
- 最近进程使用：S1 深度模拟器改动之前的旧二进制
- Round 2 状态：数学方案与实施契约完成
- Round 3 状态：日志、归因、指标与统计比较契约完成
- Round 4 状态：S2/M7 数学求解层完成第四轮打磨
- Round 5 状态：模拟器数字孪生、策略控制、因果日志与真实性指标完成第五轮联合打磨
- Round 6 状态：固定锚点均值回归核心假设的独立统计验证协议完成
- Round 7 状态：结构可识别性、有限样本模拟器不确定性、稀有破产事件与时间一致鲁棒控制完成第七轮联合打磨
- Round 8 状态：交易所契约更新、反事实执行世界、部分识别 OPE、安全策略升级与价值信息实验设计完成第八轮联合打磨
- Round 9 状态：Orderbook-EWMA 自反馈、博弈内生性、开盘终端风险、整数动作优化与闭环鲁棒证书完成第九轮联合打磨
- Round 10 状态：模拟器、策略、求解器、日志、指标与实验治理的不可变核心、可变组件和单向依赖完成第十轮架构打磨
- Round 11 状态：实盘等价性、多时间尺度控制、机制可识别模拟校准、黑天鹅生存与安全域内多目标收益优化完成第十一轮联合打磨
- Round 12 状态：固定外部锚均值回归核心假设的可证伪实验、Price Discovery 对照、Orderbook-EWMA 机制识别与经济可交易性判定协议完成；尚未修改 engine 代码，尚未启动新实验
- Round 13 状态：P0 正确性落地。阈值诊断拆分为 `warming_up`、`insufficient_data`、`invalid_input`、`model_failure`、`ready`；预热采用保守先验并显式记录；运行时新增 maker-only reduce-only flatten 与最终结算状态；重复 R1-R7 不进入默认矩阵；GNU 工作区测试通过。
- Round 14 状态：可审计研究方法纯函数、类型化锚点/规则、episode 竞争风险、cluster bootstrap、X0–X6 状态机与 GNU 工具链落地。
- Round 15 状态：Price Discovery、self-excluded LOB、post-fill edge、因果对照、survival 和总判定计算器接入运行产物。
- Round 16 状态：M9 完整数学与工程规格冻结；代码、校准、消融和实盘验证尚未开始，不授予交易权限。

## 1. 不可变原则

### 1.1 交易对象与核心机制

- 交易对象为 Binance USDⓈ-M TradFi 永续合约。
- A 股/港股官方收盘价是固定锚点。
- 股票闭盘期间，合约相对固定收盘锚点的偏离构成均值回归信号来源。
- 固定锚点不能被合约价格反向改写，不能使用未来信息或事后修正。
- M1 到当前最高版本必须在同一公共事件流上并行运行。
- 每一版实验、每一个 ledger、每一个标的的原始记录和指标必须保留。

### 1.2 安全与执行边界

- 行情、锚点、FX、资金费、账户、交易所规则、订单状态任一未知或过期时，默认不增加风险。
- 正常报价默认只允许 Maker/Post-only。
- 股票开盘前、资金费截止前和明确的极端风险状态必须降低风险。
- Simulation、Replay、Backtest、Live 使用同一策略决策契约；只有执行适配器不同。
- Simulation 不读取真实凭证，不调用真实下单接口。
- 不把短期收益、成交率或单次实验结果当作稳定正期望证明。
- 不使用深度学习替代当前可解释策略热路径；新方法必须可审计、可回滚、可消融。

## 2. 可变部分

### 2.1 模拟器可变部分

- 事件接收、排队、乱序、重复、丢失、断线和重连语义。
- 深度盘口、队列位置、同价位撤单/新增/成交和流动性坍塌。
- 订单 ACK、Post-only 拒绝、撤单 ACK、撤单竞态、部分成交和对账。
- 市场到决策、决策到交易所、撤单到交易所的延迟分布。
- 手续费、资金费、合约规格、保证金和清算缓冲。
- 共享事件流、写盘、回放、故障注入和指标口径。

### 2.2 策略可变部分

- 入场门槛、信号置信度和方向性逆向选择惩罚。
- 仓位目标、组合集中度、动态资金和风险预算。
- 固定锚点下的残差状态、单边扩散识别和持仓时限。
- 正常减仓、风险减仓、资金费前减仓和极端退出策略。
- M1–M6 的可解释消融组件；核心锚定原则不得被改变。

## 3. Round 1：当前结果的事实基线

本轮采用的 M6 最新快照约为：策略损益 +0.48 USDT，市场持仓损益 -5.17 USDT，资金费 -0.21 USDT，手续费 -0.23 USDT，净损益 -5.13 USDT。M6 的主要亏损来自 HK0625USDT 和 MINIMAXUSDT：

- HK0625：固定锚点约 42.77，Mark 约 40.28，系统持有多头，单标的净损益约 -4.35 USDT。
- MINIMAX：固定锚点约 45.87，Mark 约 48.21，系统持有空头，单标的净损益约 -4.59 USDT。
- 两者的 Mark/Index 仍然接近，行情质量被判定为 Fresh，但锚点残差已经约为 5%–6%。
- 这说明当前数据质量门禁正常工作，但没有识别“固定锚点可能暂时失效或持续偏离”的风险。

M6 动态资金快照还出现了目标资金与实际名义仓位不一致：

- CXMT：分配约 125 USDT，实际持仓约 200 USDT。
- MINIMAX：分配约 117 USDT，实际持仓约 283 USDT。
- UNITREE：分配约 235 USDT，实际持仓约 296 USDT。
- 动态资金当前主要限制后续加仓，没有对已有持仓形成强制硬上限。

M6 约有 3322 次 capital_rebalance、550 次挂单和 528 次撤单。60 秒刷新下几乎每次刷新都改变分配，说明动态资金缺少滞回、最小变化阈值和冷却机制。## 4. Round 1：已确认问题

### 4.1 策略与执行耦合问题

当前减仓分支存在确定性方向矛盾：

- 多头减仓意图使用 bid 价格卖出。
- 空头减仓意图使用 ask 价格买入。
- Post-only 校验却要求卖价不低于 ask、买价不高于 bid。
- 因此严格 Maker 校验会拒绝上述减仓单。

结果是 M5/M6 可以进入 reduce-only，但并不一定能真正减少已有仓位。必须在下一次实验前修复并单测。

### 4.2 策略风险模型问题

- 当前尾部风险主要监控短期波动、Mark/Index 偏离和价差，没有把 Anchor residual、残差速度和持续时间作为核心风险。
- 以“偏离越大、收益空间越大”的线性直觉处理固定锚点，无法识别单边新闻/重新定价行情。
- 动态资金风险分数没有充分包含锚点残差、资金费绝对成本、当前持仓、保证金和组合集中度。
- 动态目标下降时没有明确的“先减仓至目标、再恢复增仓”状态机。
- rejected_entries 主要记录内部策略门禁触发，不应继续与交易所拒单混用。

### 4.3 模拟器时间与撮合问题

- 当前事件循环中调用 now_ms() 的位置更接近“主循环取出事件时间”，不一定是真实 WebSocket 收包时间。
- 订单到达时间使用本地接收时钟，而成交判断仍使用 trade.event_time_ms，存在时钟语义混用。
- BookTicker/Mark 触发 rebalance 时部分使用交易所事件时间，撤单和动态资金又使用本地时间。
- 固定 queue_ahead 不能表达同价位新增、撤单、成交和自身排队位置变化。
- 深度断档目前偏向终止整个实验，尚未实现标的级冻结、重新快照、对账和恢复。
- 共享事件丢失不能等到实验结束才发现；无限运行中应立即冻结或停机。
- 订单生命周期还需要更完整地区分意图、发送、交易所确认、拒单、部分成交、撤单请求和撤单确认。## 5. 模拟器方案：下一轮实施边界

### 5.1 统一事件时钟与事件封装

所有实时和回放事件统一封装为 MarketEnvelope：

- 原始事件内容；
- exchange_event_time；
- exchange_transaction_time；
- local_receive_time；
- ingress_sequence；
- connection/shard/feed 标识；
- 是否经过重连、缓存或重放。

订单到达、成交资格、撤单竞态和策略决策全部基于明确的同一模拟时间轴。禁止直接拿交易所事件时间与本地到达时间混合比较。

### 5.2 盘口与队列

- REST snapshot 与 diff-depth 序列严格校验。
- 盘口断档后只冻结受影响标的，撤销其增加风险的订单并重新同步。
- 在每个价格档维护显示数量、估计前方队列和自身剩余队列。
- 处理同价位新增、减少、撤单和 aggressed trade 的先后关系。
- 记录成交前盘口、成交后盘口和成交后 1s/5s/30s markout。
- 真实深度未知时采用保守区间，不用乐观的 top-of-book 直接成交。

### 5.3 订单生命周期与故障

- 建立统一 OrderState：Intent、PendingSubmit、New、PartiallyFilled、Filled、PendingCancel、Canceled、Rejected、Unknown。
- Post-only 在订单到达交易所时重新检查，不能只在本地生成时检查。
- 撤单请求到确认之间允许真实竞态成交。
- 交易所用户数据流与 REST open-orders/position snapshot 对账。
- 订单状态不确定时进入 NO_ACTION 或 REDUCE_ONLY，不能继续增加仓位。
- 共享事件丢失、深度断档、写盘丢失和对账失败必须即时记录 simulator_halt。

### 5.4 断线与恢复

- 断线不应默认伪造连续行情。
- 受影响标的取消增仓意图并冻结撮合。
- 重新连接、REST 快照、序列接续、订单/持仓对账成功后才恢复。
- 恢复过程和恢复前后的数据空洞必须进入指标和报告。
- 其他标的在风险隔离允许时继续运行；全局账户异常时才升级为全局 HALT。

### 5.5 模拟器验收指标

除 PnL 外，必须记录：

- 事件丢失、重复、乱序、断线、重连和深度重同步次数；
- 订单提交到 ACK、撤单到 ACK、未知状态持续时间；
- 队列等待时间、成交前队列量、部分成交比例；
- 1s/5s/30s markout、逆向选择成本和流动性坍塌损失；
- Post-only 拒单、撤单竞态成交、对账差异；
- 每标的及组合的最大持仓、保证金占用和清算缓冲；
- 同一事件流重复回放的一致性差异。

## 6. 策略方案：下一轮实施边界

### 6.1 固定锚点下的残差状态

锚点 A 不变，但新增可交易状态：

- residual：合约价格相对 A 的偏离；
- residual_velocity：偏离扩大或收敛速度；
- residual_duration：偏离持续时间；
- time_to_equity_open：距离股票开盘的时间；
- mark/index gap、盘口深度和资金费成本；
- 当前持仓是否处于“逆残差扩大方向”。### 6.2 风险状态机

采用 TRADING → CAUTION → REDUCE_ONLY → HALT，恢复使用滞回和冷却：

- TRADING：允许小规模正常 Maker 交易。
- CAUTION：减少新增风险，要求更高边际和更强回归证据。
- REDUCE_ONLY：取消增仓单，只允许降低已有仓位。
- HALT：停止新订单，等待数据、锚点或账户状态恢复。

Anchor residual 变大不能简单理解为更好的收益机会。残差越大时，新增仓位应先缩小；只有出现残差收敛速度、订单流和成交后 markout 同时改善，才允许恢复规模。

### 6.3 动态资金

M6 改为三层约束：

1. 组合风险预算：控制总名义、净方向和相关集中度。
2. 标的目标预算：根据波动、残差、资金费、深度和尾部状态分配。
3. 成交后硬边界：动态目标下降时立即取消增仓意图；实际仓位超过目标时强制进入减仓流程。

动态资金加入：

- 最小权重变化阈值；
- 最小仓位变化阈值；
- 再平衡冷却时间；
- 风险上升快速收缩；
- 风险下降慢速恢复；
- 不合格标的的新风险权重为零，不能用最低权重继续制造风险。

### 6.4 减仓和极端生存

- 严格 Maker-only 的正常减仓必须使用正确的非交叉报价方向。
- 资金费前、股票开盘前和硬风险突破时，必须有明确的退出可执行性检查。
- 如果坚持绝不吃单，系统必须把“无法及时退出”作为显式风险，而不是默认为已保护。
- 建议把 emergency flatten 设计成独立执行策略：正常交易不使用，只有硬风险、账户风险或清算风险触发；其成本单独统计，不混入普通策略收益。

## 7. 下一轮实验规则

下一轮不得单独跑某一个方法，必须完整并行：

- F1_m1、F2_m2、F3_m3、F4_m4、F5_m5、F6_m6；
- R1_m1、R2_m2、R3_m3、R4_m4、R5_m5、R6_m6；
- 仍使用同一公共行情、FX、时间轴和故障注入结果；
- 新输出目录独立保留，不能覆盖 M6 第一版；
- 模拟器修复和策略修复必须分别标记，不能把两类收益变化合并解释。

验收顺序：

1. 先通过 GNU 编译、单元测试、确定性回放和故障注入测试。
2. 再用修复后的模拟器跑完整 M1–M6。
3. 再比较策略改动前后；不得用旧模拟器结果与新模拟器结果直接排名。
4. 只有在保守成交、断线、深度坍塌和单边锚点偏离情景下仍改善，才保留策略改动。

## 8. 待讨论决策

### 决策 A：极端退出

推荐：正常交易 Maker-only；硬风险状态允许独立 emergency flatten，并单独统计成本。若不采用，必须接受极端情况下无法退出的尾部风险。

### 决策 B：锚点残差处理

推荐：锚点继续固定，但残差超过压力区后只减仓，不新增；恢复需要残差收敛和流动性恢复的联合证据。

### 决策 C：实施顺序

推荐：先修模拟器时间/盘口/订单生命周期，再修策略仓位和锚点风险；否则新旧结果无法归因。

## 9. 后续轮次记录模板

### Round N

- 日期：
- 方案目标：
- 模拟器改动：
- 策略改动：
- 不可变原则检查：
- 单元测试/回放测试：
- 完整矩阵实验目录：
- 结果：
- 保留项：
- 淘汰项：
- 尚未解决问题：
- 下一轮需要讨论的决策：

## 10. Round 2：现实约束与“最优解”的定义

### 10.1 本轮目标与边界

- 模拟器目标：复现“如果同一订单在真实 Binance 到达，会发生什么”，不负责提高策略收益。
- 策略目标：在模拟器给定的真实执行分布下，先保证生存，再最大化长期净增长。
- S2 是模拟器版本，不改变 M1–M6 策略规则；策略新增模块进入 M7，M6 永久保留作为动态资金第一版。
- 新实验仍必须并行跑 F1_m1…F7_m7 与 R1_m1…R7_m7；不得只跑 M7。
- 本轮只确定数学模型和实现契约，不改代码，不重启实验。

“最优”定义为词典序优化，而非单一加权分数：

\[
\text{第一层：满足所有硬安全约束；}\qquad
\text{第二层：在可行域内最大化最坏分布下的长期净增长。}
\]

非平稳、部分可观测市场中无法声称永久全局最优。本项目追求的是每个决策时点的鲁棒滚动最优，并通过保守回放、压力情景和持续校准逼近实盘最优。

### 10.2 现实资料带来的硬结论

- Binance diff-depth 只有在“pu == previous_u”时才连续；断链必须重新初始化本地簿。
- 普通深度快照不包含 RPI 流动性；显示盘口不等于全部可成交流动性。
- GTX/Post-only 必须在订单抵达交易所时按当时盘口判断，而非本地决策时判断。
- 修改限价单会重新排到撮合队尾；部分成交单的某些修改会导致取消。
- 真实执行必须同时建模 feed latency、order latency、队列位置与成交后的逆向选择。
- “成交概率高”与“成交后收益好”通常冲突，不能把 fill rate 当执行质量。

## 11. S2 模拟器数学模型（与策略完全独立）

### 11.1 单一离散事件时钟

每个市场包封装为：

\[
e_k=(t_k^{ex},t_k^{tx},t_k^{recv},seq_k,feed_k,payload_k)
\]

每个订单动作封装为：

\[
o_j=(t_j^{dec},t_j^{send},t_j^{arr},t_j^{ack},clientId_j,payload_j)
\]

- “t_ex”：交易所事件时间；“t_tx”：交易所事务时间；“t_recv”：本机单调时钟收包时间。
- “t_dec/send/arr/ack”：决策、发送、交易所抵达、确认到达本机的时间。
- 仿真器只按单一 sim_time 最小堆推进；同一时间以事件类别和 ingress_sequence 稳定排序。
- 禁止用 trade.event_time 与本地 arrival_time 直接比较。
- 实盘采集同时保存 wall clock 与 monotonic clock；回放使用记录的间隔，不使用回放机器的 now()。
- F/R 全 ledger 消费同一个不可变事件日志和同一组随机数种子（common random numbers）。

### 11.2 本地订单簿与数据可信状态

每标的维护 \(B_t=(B_t^{bid},B_t^{ask},u_t)\) 以及状态：

\[
D_t\in\{\text{SYNCING},\text{LIVE},\text{GAPPED},\text{RECONCILING},\text{HALTED}\}.
\]

- 先缓存 diff-depth，再取 REST snapshot，再从覆盖 lastUpdateId 的事件开始接续。
- 每个新事件必须满足序列连续；价格档数量是绝对量，零数量删除。
- gap、交叉簿、过期、重复冲突只冻结受影响标的；账户状态未知才全局 HALT。
- 冻结时取消增仓意图、停止模拟新成交；重建深度并完成订单/仓位对账后，以滞回恢复。
- 事件队列溢出必须在发生时触发 freeze/halt，不能等无限运行结束才报告。
- RPI/隐藏流动性作为不可观测量处理，不允许用普通 L2 快照假定“看见了全部队列”。

### 11.3 L2 队列与成交区间

订单在价格 \(p\) 抵达时，前方队列不是常数，而是区间：

\[
Q^{ahead}_{0}(p)\in[\rho_L Q^{disp}_{arr}(p),\rho_U Q^{disp}_{arr}(p)].
\]

对同价位显示量减少 \(\Delta Q^-_t\)，拆为可观测主动成交 \(V_t\) 与无法归因的撤单/修正 \(C_t\)：

\[
Q^{ahead}_{t+}= \max\{0,Q^{ahead}_t-V_t-\eta_t C_t\},\quad \eta_t\in[\eta_L,\eta_U].
\]

- 主动成交优先消耗前方队列；无法确定撤单发生在我方前还是后时，保留上下界。
- 保守主结果使用不利端参数；中性与乐观只作敏感性报告，不参与上线判定。
- 当穿价成交时必须视为 adverse fill 候选；触价但未穿价只能在前方队列耗尽后部分成交。
- 聚合 100ms 深度包内无法恢复精确先后顺序时，枚举所有合法顺序并取最不利可行结果。
- 自身挂单占该档显示量或成交量超过阈值时，历史回放的“小订单不影响市场”假设失效：样本标记 invalid 或叠加冲击模型。
- \(\rho,\eta\) 和隐藏流动性参数必须由实盘 shadow order/小额探针校准，不得凭经验固定。

### 11.4 成交后的逆向选择

每笔被动成交 \(f\) 记录方向化 markout：

\[
MO_f(h)=s_f\,[m_{t_f+h}-p_f],\qquad h\in\{1s,5s,30s,open\},
\]

其中买入 \(s_f=+1\)，卖出 \(s_f=-1\)。负值表示成交后价格向不利方向移动。

仿真验收比较联合分布：

\[
(\Pr[\text{fill}],fill\ latency,partial\ ratio,MO(1s,5s,30s),cancel\ race).
\]

只有成交率接近、markout 却过度乐观的模型仍然是不合格模型。

### 11.5 订单生命周期、延迟与交易所规则

订单状态机扩展为：

\[
Intent\rightarrow PendingSubmit\rightarrow PendingAck\rightarrow
\{New,PartFilled,Filled,Rejected,Unknown\}
\]

以及 PendingCancel、Canceled、Expired、ExpiredInMatch、Liquidation/ADL。

- submit ACK、私有流更新和 REST 查询是三个独立信息源；超时不等于拒单，而是 Unknown。
- Unknown 状态禁止重复制造方向风险，先按 clientOrderId/orderId 对账。
- Post-only 在 \(t^{arr}\) 的盘口上检查；取消在 cancel-arrival 前仍可能成交。
- amend 视为失去原队列优先级；filters、tickSize、stepSize、notional、percent-price 和限频均来自版本化 exchangeInfo。
- 延迟不再是 50/100ms 常数，使用条件经验分布：
\[
L\sim F_{endpoint,feed,load,hour}(l),
\]
并保留相关性、长尾、超时和重试；压力情景直接使用高分位与断线簇。
- API 限频、WebSocket 重连、ACK 丢失、磁盘阻塞、进程退出和机器暂停均进入故障注入。

### 11.6 账户、资金费、保证金和极端事件

每个时点计算：

\[
Equity_t=Wallet_t+UPnL_t,\qquad
Buffer_t=Equity_t-MM_t-Reserved_t.
\]

- UPnL 使用交易所 Mark Price；成交盈亏使用真实成交价；两者不能混用。
- \(MM_t\) 按每标的当前风险限额/杠杆档位做分段函数，并包含跨仓组合耦合。
- 资金费在真实结算时点按持仓和最终 funding rate 扣付；手续费按 maker/taker 与实际成交量计算。
- 模拟强平触发、强平费用、保险基金/ADL 风险情景，报告最小 Buffer/Equity。
- 压力情景至少包含锚点单边偏离、深度骤降、价差跳宽、延迟长尾、断线、资金费跳变、多标的相关冲击。
- 无限运行必须有 supervisor、heartbeat、退出码和原子 checkpoint；意外退出后能判断原因并从一致状态恢复。

## 12. M7 策略数学模型（S2 验收后才实现）

### 12.1 固定锚点与可变状态

锚点 \(A_i\) 永远固定。对标的 \(i\) 定义对数残差：

\[
x_{i,t}=\log(P_{i,t}/A_i).
\]

锚点数值不变，但“此刻是否适合交易该锚”是可变隐状态：

\[
S_{i,t}\in\{N:\text{正常回归},C:\text{谨慎拉伸},R:\text{疑似重定价},B:\text{数据/市场破坏}\}.
\]

在状态 \(s\) 下使用带跳跃的状态切换 OU：

\[
dx_t=\kappa_s(\mu_s-x_t)dt+\sigma_s dW_t+dJ_t^{(s)},\qquad
\Pr(S_{t+dt}=r|S_t=s)=q_{sr}dt.
\]

- 正常状态约束 \(\mu_N=0,\kappa_N>0\)，体现固定收盘锚回归。
- R 状态允许弱回归、漂移或跳跃强度上升，但绝不修改 \(A\)。
- 观测包含残差、速度、持续时间、OFI、深度、spread、mark/index、funding、成交后 markout、距股票开盘时间。
- 用可解释的在线 Bayesian/HMM filter 得到 \(\pi_t(s)\)；禁止未来数据、整段平滑和深度学习热路径。
- 参数使用滚动稳健估计、指数遗忘和置信区间；样本不足进入 CAUTION，不补造确定性。

### 12.2 回归收益不是“偏离越大越好”

正常 OU 条件均值仅给出：

\[
\mathbb E[x_{t+h}|x_t,S=N]=x_t e^{-\kappa_Nh}.
\]

真正需要的是在持仓截止 \(H\) 前到达退出带的首达概率与尾部损失：

\[
p_{conv}=\Pr(\tau_{exit}\le H),\quad
\tau_{exit}=\inf\{u:x_{t+u}\in\mathcal E\}.
\]

大残差同时提高潜在回归收益与“已经重定价”的后验概率。入场必须满足最坏情形净边际：

\[
Edge^{robust}=\inf_{\theta\in\Theta_t}
\mathbb E_\theta[PnL_{net}\mid fill,action] > 0,
\]

其中 \(PnL_{net}\) 显式扣除手续费、资金费、成交逆向选择、退出成本、未成交机会成本和跳跃尾损。

### 12.3 执行决策：联合优化价格、数量和等待

动作不是简单方向，而是：

\[
a_t=(side,priceLevel,qty,ttl,cancel/hold,reduce).
\]

每个候选 Maker 动作计算：

\[
V(a)=p_{fill}(a)[G_{conv}(a)-Fee-Funding-AS(a)-ExitCost]
-(1-p_{fill}(a))OpportunityLoss-Risk(a).
\]

- \(p_{fill}\) 来自 S2 的状态依赖队列/首达模型，不使用固定概率。
- AS 来自同状态下 1s/5s/30s 条件 markout 分布。
- 若动作只提高成交概率但使成交后净值下降，则拒绝。
- 线性摩擦天然产生 no-trade band；残差刚覆盖 spread/fee 不足以入场。
- 每次市场事件后做有限候选集的滚动优化，保留上一订单可避免无意义撤挂。
- 正常 Maker 减多仓应在 ask 卖，减空仓应在 bid 买；当前反向实现必须先修复并单测。

### 12.4 生存优先的组合优化

第一层硬约束：

\[
\Pr_\theta(Buffer_{t:t+H}\le0)\le\varepsilon,\quad
CVaR_\alpha(DD_H)\le D_{max},\quad |q_iP_i|\le N_i^{hard}.
\]

并限制 gross、net、相关因子暴露、单标的集中度、流动性参与率、订单速率和距开盘剩余风险。

第二层在上述可行域内求：

\[
\max_{a,w}\inf_{\mathbb P\in\mathcal U_t}
\mathbb E_\mathbb P\left[\sum_{u=t}^{t+H}\log\left(1+\frac{\Delta W_u}{W_u}\right)\right]-\lambda TO(a,w).
\]

- \(\mathcal U_t\) 是由参数置信区间、压力情景和经验分布构成的 ambiguity set。
- 动态资金不再做纯 inverse-risk；使用情景损益矩阵求解 robust CVaR/增长问题。
- 风险上升立即收缩，风险下降缓慢恢复；权重和仓位均有 deadband、滞回和 cooldown。
- 新目标低于实际仓位时：先取消增仓单，再进入硬减仓，未回到上限前权重不得恢复。
- 名义仓位按当前 Mark、合约乘数和 filters 计算，不能按固定 Anchor 价格代替。

### 12.5 状态机与极端退出

策略状态保持：

\[
TRADING\rightarrow CAUTION\rightarrow REDUCE\_ONLY\rightarrow HALT.
\]

- TRADING：仅在 \(Edge^{robust}>0\) 且风险约束有余量时增加风险。
- CAUTION：提高门槛、缩短 TTL、降低目标，禁止对极端残差机械抄底/摸顶。
- REDUCE_ONLY：实际仓位超过硬目标、重定价概率过高、临近开盘/资金费或保证金恶化时触发。
- HALT：数据、订单、账户或锚点状态未知。
- 恢复必须同时满足残差收敛、流动性恢复、数据连续、订单对账和冷却完成。
- emergency flatten 仍作为独立待决安全层：普通收益完全不使用 taker；若启用，仅硬风险可触发并单独统计。

## 13. S2 与 M7 的验证设计

### 13.1 S2 模拟器验收

- 单元性质：时间单调、状态转换合法、仓位守恒、现金守恒、同种子确定性。
- Binance 规则：depth 重建、GTX 抵达拒单、amend 丢队列、cancel-race、部分成交、Unknown 对账。
- 校准：按标的/时段比较 shadow/live 的 ACK 延迟、fill、partial、queue wait 和 markout 联合分布。
- 保守性：主结果不得系统性高估 fill 或低估负 markout；区间覆盖率必须报告。
- 可靠性：进程异常退出、事件丢失、磁盘阻塞、重连和 checkpoint 恢复均有自动测试。
- S2 通过前，不得用其结果决定 M7 优劣。

### 13.2 M7 策略验收与完整消融

同一 S2 事件流并行保留 F1_m1…F6_m6、R1_m1…R6_m6，并新增 F7_m7、R7_m7。M7 内部消融至少拆分：regime、robust_edge、hard_rebalance、portfolio_cvar、execution_value；每个消融也必须共享事件、延迟样本、故障样本和随机种子。

主判定使用配对差分，而不是各跑一次后比较绝对 PnL：

\[
\Delta Y_k=Y_k(M7)-Y_k(M6).
\]

在多日、多状态、多随机种子下报告置信区间。通过条件：

1. 正常市场净收益和收益/占用资金改善；
2. 极端情景的最大回撤、最小保证金缓冲和尾部 CVaR 不恶化；
3. 收益不是由更高名义、更高成交乐观度或更少故障样本造成；
4. 每标的均报告 PnL 分解、fill/markout、持仓、资金费、费用和状态占比；
5. 任一模块没有稳定增量价值则淘汰，但 M1–M6 历史版本仍保留。

## 14. Round 2 实施顺序

1. 先修复已确认的 Maker reduce-only 报价方向 bug，并加执行契约测试。
2. 实现 S2 单时钟、真实订单状态机、标的级重同步和实时 halt。
3. 实现动态 L2 队列区间、条件延迟、保证金/清算与 supervisor/checkpoint。
4. 用旧 M1–M6 完整矩阵验证 S2；此阶段不引入 M7。
5. S2 验收后实现 M7 regime、robust edge、硬再平衡和组合风险优化。
6. 跑 M1–M7 全矩阵、多日、多种子和故障压力测试，保留全部版本。

## 15. Round 2 尚未解决的决策

- 极端退出：是否允许只在保证金/清算硬风险下使用 taker emergency flatten。
- 校准数据：是否允许用极小真实订单做 shadow/probe；若不允许，只能输出更宽、更保守的成交区间。
- 组合硬阈值：\(\varepsilon,\alpha,D_{max}\)、最小保证金缓冲和单标的集中度需要在 1 万元人民币账户尺度下确定。
- M7 的首个实现应保持可解释 HMM/稳健滤波，不引入神经网络。

## 16. Round 2 主要资料

- [Binance：本地订单簿同步](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
- [Binance：USDⓈ-M WebSocket 交易与 GTX/改单](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade)
- [Binance：市场流与 diff-depth/RPI](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)
- [Binance：ADL Risk](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
- [HftBacktest：延迟、L2/L3 与队列回放](https://hftbacktest.readthedocs.io/en/py-v2.1.0/)
- [NautilusTrader：同核心组件回测](https://nautilustrader.io/docs/latest/concepts/backtesting/)
- [Fill probabilities under state-dependent order flow, 2026](https://arxiv.org/abs/2403.02572)
- [Fill probability vs post-fill return, 2025](https://arxiv.org/abs/2502.18625)
- [Market simulation under adverse selection, 2024](https://arxiv.org/abs/2409.12721)
- [Optimal trading of microstructure mean reversion, 2026](https://arxiv.org/abs/2608.00885)
- [Model-free excursion analysis of mean reversion, 2026 version](https://arxiv.org/abs/2011.02870)
- [Optimal mean reversion with costs and stop-loss](https://arxiv.org/abs/1411.5062)
- [Microstructural FIFO LOB stochastic control](https://arxiv.org/abs/1705.01446)

## 17. Round 3：日志、归因与评价体系

### 17.1 本轮目标

Round 3 不改变固定锚点原则，也不提前实现 M7。目标是建立四个彼此独立、又能用因果 ID 连接的系统：

1. Simulator Truth：市场、交易所、网络和账户发生了什么。
2. Strategy Intent：策略看到了什么、计算了什么、为何选择或拒绝某动作。
3. Economic Ledger：每一分钱、每一单位仓位如何变化。
4. Evaluation：在共同机会集上，哪个方法在何种状态下真正更好。

必须禁止以下混淆：

- 模拟器造成的成交变化，不能归因成策略 alpha。
- 内部门禁不能记为 exchange reject。
- 未成交不能只写“没有订单”，必须有具体 reason code 和候选动作价值。
- 实时近窗指标不能替代全程累计指标。
- 单个总分不能让高收益抵消强平、数据损坏或不可恢复状态。

### 17.2 当前实现审计结论

- AuditRecord 只有 sequence、单时间戳、kind、symbol、reason、correlation_id，无法表达完整因果链与多时钟。
- SimulationRecord 只记录 placed/canceled/fill/funding/rebalance 等少数结果，没有 feature、候选动作、门禁分解、ACK、队列变化和状态转换。
- rejected_entries 同时承载“无可接受信号”和本地 maker 校验失败，命名与口径错误。
- metrics.json 的 history 只保留 900 点；长跑会丢弃早期 equity peak，最大回撤和 Sharpe 只反映近窗。
- 当前 Sharpe/Sortino 对 30 秒 PnL 增量直接按全年秒数年化；序列相关、重叠持仓和非独立样本会造成虚高。
- run-manifest 没有 git commit、dirty diff hash、数据 hash、配置 hash、schema 版本、随机种子、退出原因和退出码。
- records 写盘允许 drop，且不能立刻在 ledger 级冻结；这不满足审计账本要求。

## 18. 四层可观测架构

### 18.1 A 层：不可变原始事实

按来源分别记录 append-only 原始流：

- market_raw：depth、bookTicker、aggTrade、mark/index、funding；
- account_raw：ORDER_TRADE_UPDATE、TRADE_LITE、余额、仓位、ADL、REST reconciliation；
- reference_raw：官方收盘锚、交易日历、FX、exchangeInfo、费率与杠杆档位；
- system_raw：连接、DNS/代理、限频、队列、磁盘、进程、时钟偏移和异常。

原始记录必须包含 raw payload 或其内容寻址引用、source、connection_id、exchange sequence、exchange event/transaction time、local receive wall/monotonic time、ingress sequence、CRC/hash。

### 18.2 B 层：标准化领域事件

统一 Envelope：

\[
E=(schema,run,source,eventId,parentId,traceId,seq,times,entity,type,payloadHash,payload).
\]

所有领域事件使用整数 tick/quantity 和记账币最小单位。显示用小数是派生值，不能成为账本真值。

关键 ID：

- run_id、run_id、ledger_id、strategy_version、simulator_version；
- market_event_id、decision_id、candidate_id、risk_eval_id；
- intent_id、client_order_id、exchange_order_id、fill_id；
- position_lot_id、rebalance_id、incident_id、checkpoint_id；
- trace_id 和 parent_event_id。

同一市场事实只写一次；各 M1–Mxx ledger 通过 market_event_id 引用，避免复制后发生漂移。

### 18.3 C 层：经济账本与派生事实

经济账本必须双轨：

- Accounting Ledger：严格可加总、可对账的现金/仓位事实。
- Model Attribution：依赖策略模型的预测贡献，必须标明 model_version，不能覆盖会计事实。

每个 fill 创建不可变成交 lot。账户聚合仓位可按 Binance 口径维护，但分析层同时保留 lot 生命周期。任何时点满足：

\[
Equity=Wallet+UPnL,\qquad
\Delta Equity=TradingPnL+Funding-Fees-LiquidationCost+Transfer.
\]

持仓期间使用精确分解：

\[
q\Delta Mark=q\Delta Index+q\Delta(Mark-Index).
\]

进一步把 \(q\Delta Index\) 按路径事实分为 residual 收敛段与扩散段；模型归因另分为：

\[
q\Delta Index=q\,\mathbb E_t[\Delta Index]+q(\Delta Index-\mathbb E_t[\Delta Index]).
\]

前者是预测贡献，后者是意外冲击；两者都不能和成交执行成本混算。

### 18.4 D 层：在线指标与离线研究

- live_snapshot.json：只服务监控，允许近窗统计。
- cumulative_state.json：全程可合并统计量、历史峰值、全程回撤和计数，永不因 ring buffer 丢失。
- equity_curve：独立分段时序文件，不嵌入一个不断膨胀的 metrics.json。
- metrics_final：结束/恢复点生成全程结果。
- metric_cube：method × symbol × session × regime × direction × residual_bucket × fill_type。
- comparison_report：配对差分、置信区间、支配关系和失败原因。

JSONL 可保留为审计格式；大规模分析可异步压缩为 Parquet。压缩层不得成为唯一事实源。

## 19. 端到端因果日志

### 19.1 一次决策必须能完整重放

因果链固定为：

\[
MarketEvent\rightarrow FeatureSnapshot\rightarrow RegimeEstimate
\rightarrow CandidateSet\rightarrow RiskEvaluation\rightarrow Decision
\rightarrow OrderIntent\rightarrow Send\rightarrow ExchangeAck
\rightarrow QueueEvolution\rightarrow Fill/Cancel\rightarrow PnL.
\]

FeatureSnapshot 至少记录 anchor residual/velocity/duration、bid/ask/L2、mark/index、OFI、波动、funding、距开盘时间、当前仓位、目标仓位、保证金缓冲、数据年龄与质量。

CandidateAction 每个候选都记录：

- side、price level、quantity、TTL、reduce_only；
- predicted fill probability/time-to-fill；
- convergence gain、fee、funding、adverse-selection、exit cost、risk cost；
- robust edge 下界/中位/上界；
- 每条硬约束的 slack；
- chosen=false 时的精确淘汰原因。

即使最终 NO_ACTION，也必须能回答“有哪些候选、哪一项约束或哪一项成本使其失败”。

### 19.2 Reason code 规范

禁止只写自由文本 detail。使用稳定枚举和可选上下文：

- DATA.DEPTH_GAP、DATA.MARK_STALE、DATA.FX_STALE；
- ANCHOR.MISSING、ANCHOR.NOT_FINAL、ANCHOR.AGE_LIMIT；
- SIGNAL.EDGE_BELOW、SIGNAL.REGIME_REPRICE、SIGNAL.CONFIDENCE_LOW；
- RISK.POSITION_CAP、RISK.CVAR、RISK.MARGIN_BUFFER、RISK.CONCENTRATION；
- EXEC.POST_ONLY_CROSS、EXEC.QUEUE_TOO_LONG、EXEC.CANCEL_RACE；
- EXCHANGE.REJECT_FILTER、EXCHANGE.REJECT_RATE_LIMIT、EXCHANGE.UNKNOWN；
- SYSTEM.WRITER_BACKPRESSURE、SYSTEM.CLOCK_JUMP、SYSTEM.PROCESS_EXIT。

每个 code 单独计数；“策略拒绝、本地校验、交易所拒绝、模拟器拒绝”必须四分。

### 19.3 订单与队列日志

每个订单记录全部状态迁移及前后状态：

- intent_created、submit_enqueued、wire_sent、exchange_arrived；
- ack_new、partial_fill、fill、cancel_requested、cancel_arrived、cancel_ack；
- rejected、expired、expired_in_match、unknown、reconciled；
- transition_from、transition_to、trigger_event_id、latency component。

QueueEvent 记录 price、displayed_before/after、trade_qty、cancel_qty、estimated_ahead_low/base/high、own_remaining、fill_low/base/high 和归因假设。

Post-only reject 使用订单抵达时盘口快照；cancel-race 必须关联 cancel_request 与导致成交的 market_event。

### 19.4 PnL 与风险日志

每次仓位、mark、funding、fee、margin 变化都写 LedgerEntry：

- amount、currency、component、symbol、fill/lot/order/decision ID；
- realized/unrealized、before/after balance、before/after position；
- mark/index/anchor、margin tier、maintenance margin、buffer；
- exact_accounting=true/false。

PnL component 至少包含：

- convergence_aligned、residual_divergence、mark_index_basis；
- entry_execution、exit_execution、spread_capture、adverse_markout；
- maker_fee、taker_fee、funding_regular、funding_special；
- emergency_exit、liquidation、rounding/filter、FX；
- unexplained_reconciliation。

所有 component 必须精确加总到总 equity 变化；unexplained 不得被静默归零。

### 19.5 系统可靠性和存储保证

- audit/accounting writer 禁止 drop；背压超过阈值时冻结新增风险。
- market raw 队列溢出立即标记具体 symbol gap；不能只在运行结束检查 AtomicU64。
- 文件分段、原子 rename、checksum、尾部截断恢复和 schema migration 必须测试。
- 每个 segment 记录前一段 hash，形成可验证链；checkpoint 保存最后 committed sequence/hash。
- supervisor 记录启动命令、父进程、心跳、signal/exception、退出码、stderr 尾部和恢复结果。
- 日志中 API key、secret、签名、代理凭证永不落盘；client order id 可以落盘。
- 运行 manifest 固化 git commit、dirty_patch_hash、GNU toolchain、binary hash、config hash、data hash、random seed、时区和依赖版本。
- 任何恢复都生成新 run_attempt_id，但保持同一 run_id，禁止伪装成从未中断。

## 20. 指标体系：先验分层，不做单一总分

### 20.1 Level 0：数据与模拟器合格门

任一项失败，该 run 不进入策略排名：

- event loss/duplicate/conflict、sequence gap、crossed book、clock reversal；
- raw-to-domain 完整率、ledger reconciliation error；
- order state illegal transition、unknown duration；
- writer drop、checkpoint corruption、nondeterministic replay hash mismatch；
- fee/funding/filter/margin rule version missing。

模拟器准确度按真实 shadow/live 联合分布校准：

- latency：p50/p90/p99/p99.9、Wasserstein distance；
- fill：Brier score、log loss、reliability curve、ECE；
- time-to-fill：生存曲线、censoring-aware integrated Brier score；
- queue：预测区间覆盖率和宽度；
- markout：1s/5s/30s 分布距离与尾部分位误差；
- cancel race、partial fill、post-only reject 的频率误差。

### 20.2 Level 1：生存与尾部风险

这是策略排名第一层，不能被收益抵消：

- liquidation/ADL 次数、risk-of-ruin 上界；
- minimum margin buffer、buffer/Equity 的 p1/p5；
- max drawdown、drawdown duration、time under water、recovery time；
- VaR/CVaR/expected shortfall、worst session、worst excursion；
- jump loss、gap loss、disconnect loss、emergency-exit cost；
- 压力场景损失：残差 5%/10%/20%、深度 -50%/-90%、延迟 p99.9、相关性趋近 1；
- 超硬仓位时间、目标超调面积：
\[
OvershootArea_i=\int (|N_{i,t}|-N^{hard}_{i,t})_+dt.
\]

### 20.3 Level 2：经济收益与资本效率

- realized、unrealized、gross、net PnL 及完整 component；
- total return、session return、return on allocated capital；
- return on average/peak margin、PnL per gross-notional-hour；
- profit factor、payoff ratio、win/loss excursion、expectancy；
- fee drag、funding drag、execution drag、adverse-selection drag；
- exposure time、gross/net exposure、turnover、capacity/participation；
- Calmar、Omega、Ulcer index；Sharpe/Sortino 仅作辅助，不作唯一结论。
- 好市场收益定义为“有效机会强度上分位且数据/流动性正常”条件下的净收益与机会捕获率，而不是事后挑选上涨行情。

### 20.4 Level 3：信号与状态模型

- canonical opportunity count、action coverage、entry frequency；
- 按 reason code 的拒绝漏斗；
- \(p_{conv}\) 的 Brier/log loss、校准斜率/截距、ECE；
- predicted edge 与 realized net edge 的 MAE、bias、rank correlation；
- half-life/first-passage 预测误差；
- regime 后验稳定性、切换次数、停留时间、各状态条件 PnL；
- residual bucket、方向、距开盘/资金费时间分桶表现；
- false mean-reversion entry：入场后先触发 stop/risk state 而未进入退出带。

### 20.5 Level 4：执行质量

- submit/ACK/cancel/reconcile latency 全分位；
- fill rate 必须按价格档、队列分位、状态、方向条件化；
- time-to-first-fill、time-to-complete、partial ratio、unfilled expiry；
- implementation shortfall：
\[
IS=s(P_{fill}-M_{decision}),
\]
买入 \(s=+1\)，卖出 \(s=-1\)，正值为成本。
- price improvement、effective spread、realized spread；
- 100ms/1s/5s/30s/open markout 与 adverse-fill ratio；
- queue wait、queue ahead depletion、cancel-to-fill race；
- quote age、reprice count、cancel/order ratio、amend/order ratio；
- maker/taker ratio、post-only reject、filter reject、rate-limit reject；
- reduce-only 完成率、达到硬目标耗时、退出 shortfall。

### 20.6 Level 5：组合和动态资金

- target vs actual notional、tracking error、overshoot duration/area；
- rebalance count、有效变更率、无效 churn、换手成本；
- 单标的权重、HHI、最大集中度、边际 CVaR 贡献；
- gross/net、方向偏置、共同因子暴露、相关冲击贡献；
- 每单位风险预算的净收益、风险预算利用率；
- 风险上升收缩延迟、风险下降恢复延迟；
- 资金从被降权标的转出后带来的增量 PnL 与避免损失；
- dynamic capital 相对固定等权的配对增量，而非只看 M6/M7 自身绝对收益。

### 20.7 Level 6：运行与运维

- uptime、feed availability、symbol freeze/HALT 时间占比；
- reconnect/resync/reconcile 次数与恢复时间；
- event/decision/order/log throughput，队列 high-water mark；
- CPU、memory、disk bytes、fsync latency；
- heartbeat miss、process restart、checkpoint recovery point objective；
- 指标生成延迟和报告完整率。

## 21. 方法优劣的统计比较

### 21.1 共同机会集与独立样本

所有方法在 canonical decision epochs 上评估。即使某方法不交易，也要保存其候选动作和 counterfactual value，避免只比较各自成交样本造成选择偏差。

统计基本单位优先使用：

- 一个完整股票闭盘 session；
- 一个独立 anchor-residual excursion；
- 一个预注册压力情景 × seed。

禁止把每个 tick、每次刷新或重叠 30 秒 PnL 当独立样本。现有“30 个样本即可 Sharpe”的规则淘汰。

### 21.2 配对估计

对方法 \(a,b\)，在完全相同市场事件、延迟样本、故障样本与随机种子上计算：

\[
\Delta Y_k=Y_{a,k}-Y_{b,k}.
\]

报告 mean/median、Hodges-Lehmann effect、block-bootstrap 置信区间、胜率和：

\[
P(\Delta Y>0),\qquad P(\Delta Risk\le0).
\]

时间相关性使用 session/excursion block bootstrap 或 HAC；普通 IID 标准误禁止用于高频序列。

### 21.3 多重试验和数据窥探

- manifest 预注册 primary metric、baseline、版本集合和淘汰门槛。
- Sharpe 使用自相关修正并报告置信区间；同时报告 Probabilistic Sharpe Ratio。
- 多版本、多参数搜索必须报告 Deflated Sharpe Ratio。
- 全方法族对基线使用 White Reality Check 或 Hansen SPA，控制“试得越多越容易偶然赢”。
- 调参窗口、验证窗口、最终锁箱窗口严格分离；锁箱失败不得回头改口径。
- 参数敏感性以稳定平台为优，不选择尖锐单点最优。

### 21.4 支配规则

方法 A 只有同时满足才可称优于 B：

1. 两者均通过 Level 0；
2. A 所有生存硬约束通过；
3. primary net-return/capital-efficiency 的配对增量置信下界大于预设最小经济效应；
4. CVaR、max drawdown、margin buffer 没有超过容忍度的恶化；
5. 改善不只来自一个标的、一个 session、一个方向或一个极端 fill；
6. 在成本、延迟、队列三组保守参数下结论方向稳定。

否则标记为 trade-off、inconclusive 或 rejected，不能只按净 PnL 排名。

## 22. 报告矩阵与深度归因输出

每次完整实验自动生成：

1. Executive survival sheet：每方法硬门是否通过、失败原因。
2. Method × Symbol 主表：全部收益、风险、执行、信号、资本指标。
3. Pairwise delta table：M2−M1、M3−M2…M7−M6 及 F/R 配对差。
4. Attribution waterfall：总 PnL 到 convergence/divergence/basis/execution/funding/fee/emergency。
5. Drawdown episodes：前 N 次回撤的起点、谷底、恢复、标的和因果事件。
6. Fill diagnostics：队列、成交概率、markout、取消竞态校准图。
7. Regime cube：状态 × 标的 × 方向 × residual bucket。
8. Rejection funnel：每级 gate 的机会数、拒绝数和潜在/避免损失。
9. Capital diagnostics：目标/实际、超调面积、集中度、边际 CVaR。
10. Reliability report：gap、drop、重连、冻结、退出、恢复与校验和。
11. Reproducibility report：commit/config/data/binary/hash/seed 与 replay hash。
12. Incident bundle：任一大亏、unknown order、对账差异可一键导出完整 trace。

每个表同时提供 aggregate、per-method、per-symbol、per-session。总表不得替代单标的结果。

## 23. Round 3 实施分期

### L1：先修口径，防止继续产生不可解释数据

- 拆分 strategy_block/local_validation/exchange_reject/simulator_reject。
- 修复全程累计回撤；近窗和 lifetime 指标分离。
- manifest 增加版本/hash/seed/exit metadata。
- 运行错误、退出来源、writer drop 立即落盘。
- 修复 Maker reduce-only 方向 bug。

### L2：事件溯源骨架

- 实现统一 Envelope、因果 ID、ReasonCode 和订单状态事件。
- 实现 Accounting Ledger、不变量校验和全链 trace。
- shared market facts 与 ledger decisions 分离引用。
- 分段写盘、checksum、checkpoint、恢复测试。

### L3：深度归因和指标

- candidate action、risk slack、queue evolution、markout、PnL component。
- metric cube、paired delta、block bootstrap、DSR/SPA。
- 自动生成每方法每标的报告和 incident bundle。

### L4：再进入 S2/M7

- 用新日志体系验证 S2 真实性。
- S2 合格后实现 M7；随后完整并行 M1–M7。
- 每轮保留旧输出、旧 schema reader 和 migration 记录。

## 24. Round 3 验收标准

- 任一 fill 可从原始市场包追溯到 feature、decision、order、queue、PnL。
- 任一 NO_ACTION 可给出候选动作、逐项成本和首个失败硬约束。
- 任一总 PnL 可精确分解，未解释差异为零或显式 reconciliation error。
- 任一指标可定位到原始事件集合、公式版本与聚合维度。
- 任一方法比较均使用同一机会集和配对样本。
- 长跑删除内存 history 后，lifetime max drawdown、peak equity 和累计统计不变。
- 重放两次，事件 hash、订单状态、账本和指标完全一致。
- 人为注入 drop/gap/clock jump/disk full/process kill 时，系统按契约 freeze/halt 并留下完整证据。
- F/R 若理论上只差一项配置，除此项外的共享输入 hash 必须一致。
- 报告必须覆盖全部方法、全部 7 个标的和全部预注册分层，不允许只汇报赢家。

## 25. Round 3 研究依据

- [Binance USDⓈ-M 市场流：aggTrade 为 100ms 聚合且不含 ADL/保险基金交易](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)
- [Binance ORDER_TRADE_UPDATE 状态与 STP](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/faq/stp-faq)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [Lo：The Statistics of Sharpe Ratios](https://simulations.ssrn.com/sol3/simulations.cfm?abstract_id=377260)
- [Bailey 与 López de Prado：Deflated Sharpe Ratio](https://simulations.ssrn.com/sol3/simulations.cfm?abstract_id=2460551)
- [White：Reality Check for Data Snooping](https://users.ssc.wisc.edu/~behansen/718/White2000.pdf)
- [Hansen：Superior Predictive Ability Test](https://www.tandfonline.com/doi/abs/10.1198/073500105000000063)
- [Newey-West：HAC covariance](https://simulations.ssrn.com/sol3/simulations.cfm?abstract_id=225071)

## 26. Round 4：数学最优解的边界与总决策

“最优”只允许指：在明确的信息集、动作集、校准样本、威胁集与计算预算内的可复现最优；不宣称对未知真实市场全局最优。无限跳跃、交易所停机、规则突变下不存在无条件存活保证。

采用词典序目标：

1. 数据、交易规则、账本和模拟器通过资格门；
2. 在预注册威胁集内满足生存与可执行硬约束；
3. 在可行域内最大化最坏分布下的长期净对数增长；
4. 主目标近似相同时，依次选择更低 CVaR、更低换手、更简单且更稳定的策略；
5. 若最优动作相对 NO_ACTION 的鲁棒价值差未超过估计误差，必须 abstain。

Round 4 不创建小数版本。S2 仍是下一代模拟器；M7 仍是下一代策略。M6 永久保留为动态资金第一版，下一次正式实验仍为 M1–M7 全部并行。

## 27. S2 模拟器：两种真值模式必须分开

### 27.1 Factual Replay / Simulation Truth

真实收到的 depth、trade、mark、index、funding、account 与 order update 是不可改写事实。对未观测到的撮合队列不伪造单点答案，而维护：
$Q_{ahead}(t) \in [Q_{low}(t),Q_{high}(t)]$。

每次深度变化按价格档位、方向、成交证据与可撤量，传播队列区间；每一笔自有订单同时生成 pessimistic/base/optimistic 三条一致轨迹。正式排名以 pessimistic 或预注册混合权重为主，不能事后挑路径。

自有下单仅在 exchange-arrival 时检查 GTX/Post-only；撤单与成交是 competing risks。任何 Unknown、断线、序列 gap、规则过期或账本不一致都冻结新增风险，直至 REST/stream 对账完成。

### 27.2 Generative Stress Truth

压力与反事实模式采用状态依赖的 marked Queue-Reactive Hawkes 模型。事件 $e=(type,side,level,size)$ 的强度为：

$\lambda_e(t)=softplus(\beta_e(s_t,B_t)+\sum_j\int\phi_{ej}(t-u,m_j)dN_j(u))$。

$B_t$ 包含多档队列、spread、imbalance、mark-index basis、波动与 session phase；$m_j$ 表示订单大小。核矩阵必须满足稳定/非爆炸约束，并通过 time-rescaling、残差独立性、事件类型/大小分布和压力期覆盖检验。

S2 不用生成模型替换事实回放。生成模型只用于：

- 未见压力情景、故障注入与反事实冲击；
- 估计取消竞态、流动性枯竭和自激订单流的尾部；
- 检查策略是否只利用了某个模拟器的可识别伪影。

深度 Queue-Reactive 模型只能作为 challenger。只有在锁箱数据上同时改善条件分布、尾部、路径统计和策略排名稳定性，且不破坏可审计性时，才可用于 stress ensemble；不得进入策略热路径。

### 27.3 模拟器校准不是“拟合损失最小”

校准目标是多目标约束：
$\min_\theta \sum_k w_k d_k(T_k(sim_\theta),T_k(real))$，
其中必须含 inter-arrival、size、spread、depth、imbalance、duration、return、volatility clustering、impact、markout、fill、cancel race 与极端流动性指标。

每个距离都报告样本外置信区间。若某关键统计不合格，S2 整体为未认证，不能用其 PnL 给策略升级背书。

## 28. M7 隐状态动力学：固定锚点不变，信念可变

对标的 $i$ 定义固定官方收盘锚点 $A_i$，以及
$x_i(t)=\log(P_{index,i}(t)/A_i)$。$A_i$ 在闭盘 session 内严格常数；模型只能改变“未来是否回归”的信念，不能移动锚点。

Index residual 驱动经济状态；可交易 mid、mark 分别通过观测方程加入 contract-index basis 与 mark-index basis。执行收益按真实 bid/ask/fill 结算，保证“信号价格、可交易价格、清算价格”不混为一个价格。

潜在状态 $S_i(t)\in\{Normal,Caution,Reprice,Broken\}$，采用显式持续时间的半马尔可夫模型，避免普通 HMM 的几何持续时间假设。给定状态：

$dx_i=[\kappa_s(\mu_s-x_i)+\beta_s^\top f_i]dt+\sigma_s dW_i+dJ_i$。

$J_i$ 是状态依赖的双向复合跳跃；跨标的冲击由低维共同因子和稀疏残差相关连接。Normal 中约束 $\kappa_s>0$；Reprice/Broken 允许弱回归、零回归或远离锚点。$\mu_s$ 是残差条件均衡项，不是新锚点。

观测向量只含决策时已到达的信息：residual 路径、mark-index basis、spread/depth/imbalance、订单流、波动、资金费、距股票开盘/资金费时间、数据健康和账户风险。禁止未来平滑、事后 session 分类和用成交结果回填当时特征。

## 29. 因果在线推断

M7 使用 Rao-Blackwellized 粒子/IMM 过滤半马尔可夫状态，并叠加 Bayesian Online Changepoint Detection：

$b_t(s,r,\theta)=P(S_t=s,runlength_t=r,\theta\mid\mathcal F_t)$。

参数采用分层贝叶斯收缩：全市场先验 → A/HK session 类别 → 单标的后验，解决 7 个标的单独样本不足。连续时间 OU 使用不规则间隔的精确转移密度，不把 tick 当等间隔样本。

在线只做过滤；离线可做平滑用于诊断，但平滑结果不得进入历史决策重放。后验有效样本量过低、变点概率过高或模型集合分歧过大时，状态直接进入 Caution/Broken 并缩减风险。

## 30. 从“偏离大”升级为首达概率与净边际价值

入场不再由 z-score 单独触发。对每个候选动作求联合事件：

$p_{conv}=P(\tau_{target}\le T,\tau_{stop}>T,no\ anchor\ break\mid\mathcal F_t,a)$。

其中 $\tau_{target}$ 是进入获利残差带的首达时间，$\tau_{stop}$ 是触及风险边界的首达时间。对 regime-switching jump-OU，离线用 backward Kolmogorov PIDE/动态规划生成网格，在线由后验混合和局部插值求值；网格外必须保守外推或 NO_ACTION。

候选动作的路径净收益为：

$G(a,\omega)=Convergence+SpreadCapture-ResidualDivergence-BasisMove-Fees-Funding-AdverseSelection-EmergencyExit-Rounding$。

成交不是独立伯努利。使用 fill、mid-price move、cancel 与 timeout 的 competing-risk 模型，并估计 $E[markout\mid fill,state,queue]$；否则高成交率会把逆向选择误认成优势。

定义鲁棒边际价值下界：

$LCB(a)=\inf_{Q\in U_t}E_Q[G(a)]-\lambda\sup_{Q\in U_t}CVaR_\alpha(-G(a))-model\_error(a)$。

仅当 $LCB(a)>0$、硬约束全部通过且相对 NO_ACTION 的价值差超过数值/统计误差时，才允许增加风险。

## 31. 因果 Wasserstein 鲁棒 POMDP

真实状态由市场状态、状态后验、订单/队列、仓位、现金、保证金、时钟和系统健康构成；动作集保持有限：NO_ACTION、挂单、撤单、重报价、减仓、清仓、暂停。

动态不确定性集合采用 adapted/causal Wasserstein ball：
$U_t=\{Q:AW_c(Q,\hat P_t)\le\rho_t\}$，
使对手只能使用当时信息，不能通过“未来扰动过去”制造不合法路径。$\rho_t$ 由 session-block bootstrap、锁箱覆盖误差和 regime drift 校准，不作为收益调参旋钮。

有限时域目标：

$\max_\pi\inf_{Q\in U_t}E_Q[\sum_{h=0}^{H-1}\Delta\log W_{t+h}-c_{churn}-c_{inventory}]$

并满足：

- $\sup_Q P_Q(\inf_h W_{t+h}<W_{floor})\le\epsilon_{ruin}$；
- 每条预注册 stress path 的 liquidation buffer、gross/net exposure、单标的仓位和集中度硬约束；
- 股票开盘/资金费/维护截止点的 terminal inventory 约束；
- 数据、规则、订单或账本未知时只允许 risk-reducing action；
- 下单后的最坏可达状态仍在 viability kernel 内。

概率约束只对明确定义的威胁集有效。威胁集外通过低杠杆、隔离/组合限制、fail-closed 和紧急减仓降低损失，但不伪称数学保证。

## 32. 可落地的求解器：分层而非巨型黑箱

每个决策 epoch 采用四层求解：

1. **Belief update**：过滤 $b_t$，更新变点、跳跃、成交与 markout 后验；
2. **Safety shield**：区间传播/压力场景先删除所有不可生存动作；
3. **Robust MPC**：对剩余有限首动作展开短场景树，以 common random numbers 估计 continuation value，并用 causal-DRO 对偶求最坏权重；
4. **Portfolio allocation**：7 标的联合求解风险约束对数增长，再按 lot/min-notional 取整并做可行性修复。

固定场景下，CVaR 用辅助变量写成线性约束；log-growth 为凹目标，连续仓位子问题可用内点/一阶原始-对偶法。离散报价档、TTL 和订单状态用小规模枚举/branch-and-bound，而不是训练不可解释策略。

求解必须返回 primal value、dual bound、optimality gap、约束 slack、迭代数和耗时。超过实时预算、数值失败或 gap 超阈值时，不沿用过期激进动作：只允许 NO_ACTION、撤单或风险减仓。

## 33. 动态资金的正确数学位置

M6 的动态资金是启发式目标分配；M7 将其改为联合风险预算变量 $n=(n_1,\ldots,n_7)$。在情景收益矩阵 $R$ 下求：

$\max_n\inf_{p\in P_t}\sum_s p_s\log(W+R_s^\top n)-\eta\lVert n-n_{prev}\rVert_1$

约束包含 worst-case CVaR、margin buffer、gross/net、单标的 hard cap、HHI/因子暴露和 terminal inventory。$L_1$ 调整成本、最小经济变化阈值、滞回和冷却共同抑制 M6 的分钟级再平衡抖动。

目标资金下降到现有仓位以下时，差额必须转化为有截止时间的 reduce-only liquidation schedule；不能像 M6 一样只阻止新增仓。取整后再次计算全部约束，任何超限都沿风险降低方向修复。

Risk-constrained Kelly 只决定可行域内的增长倾向；实际使用 fractional exposure，并由 ruin bound/CVaR/压力约束取最小仓位，不直接使用全 Kelly。

## 34. 不确定性校准与防过拟合

同时维护三类不确定性：

- aleatoric：跳跃、成交、价格和资金费本身的随机性；
- epistemic：参数、状态、稀疏样本与模型结构不确定性；
- operational：延迟、断线、拒单、对账和写盘风险。

后验预测区间再由按 session/regime 分层的序贯 conformal 校准修正覆盖率；有效校准样本不足时扩大区间。Conformal 只做风险校准器，不把非平稳金融序列误说成无条件 distribution-free 保证。

模型集合至少含 OU、jump-OU、semi-Markov jump-OU、经验 block bootstrap。复杂模型只有在锁箱 predictive log score、PIT/coverage、first-passage Brier、tail loss 和策略配对增量上支配简单模型，才获得权重。

所有阈值、Wasserstein 半径、风险水平、时域和压力集在锁箱前冻结。选择稳定平台，不选择单点最优；M7 内部组件用命名 feature toggle 消融，但正式版本号仍为整数。

## 35. Round 4 实施顺序与验收

实施顺序不变但进一步收紧：

1. 先完成 Round 3 L1–L3 的日志、账本、口径和可复现基础；
2. 再实现 S2 factual queue interval 与 order lifecycle；
3. 校准并认证 S2 generative stress ensemble；
4. 离线实现状态过滤、PIDE/首达概率和参数锁箱；
5. 实现 safety shield、robust MPC 与组合优化；
6. 通过 deterministic replay、故障注入和求解器差分测试；
7. 最后启动 M1–M7、7 标的、F/R 控制的完整并行实验。

新增验收条件：

- 固定锚点 hash 在整个 closed session 不变；
- 过滤器在线输出与仅使用前缀数据的离线重放逐点一致；
- PIDE 网格值与高精度 Monte Carlo 在预设误差内一致；
- DRO 半径增大时风险仓位不得反常增大；
- safety shield 对所有预注册 stress path 无约束穿透；
- 连续解取整后仍满足保证金、notional、lot 与集中度；
- solver timeout/failure 只能降低风险；
- pessimistic/base/optimistic 队列路径均可复放且不共享未来信息；
- M7 的增益不能只来自 simulator-generated data，必须在 factual replay/simulation 的公共机会集成立。

## 36. Round 4 研究依据与采用范围

- [Causal/Adapted Wasserstein DRO 的动态对偶](https://arxiv.org/abs/2401.16556)：用于时间一致的动态模型不确定性，不直接作为收益证明。
- [Queue-Reactive order book](https://arxiv.org/abs/1312.0563)：用于状态依赖队列事件与模拟器基线。
- [Deep Queue-Reactive simulator](https://arxiv.org/abs/2501.08822)：仅作为 S2 challenger，不进入策略热路径。
- [Optimal execution under incomplete information](https://arxiv.org/abs/2411.04616)：支持部分信息与 Hawkes 订单流下的执行建模。
- [Bayesian Online Changepoint Detection](https://arxiv.org/abs/0710.3742)：用于因果 run-length/变点后验。
- [带交易成本和止损的均值回归最优停止](https://arxiv.org/abs/1411.5062)：用于入场/退出边界的理论基准。
- [Risk-Constrained Kelly](https://stanford.edu/~boyd/simulations/kelly.html)：用于增长与回撤概率约束的凸近似基准。
- [Binance USDⓈ-M ADL risk](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)：ADL 风险必须进入账户压力状态。
- [Binance Futures order/account updates](https://developers.binance.com/en/docs/products/derivatives-trading-portfolio-margin/user-data-streams)：用于订单状态、清算/ADL 与账户事件真值契约。

## 37. Round 5：实盘一致性的严格定义

S2 不追求“生成一条看起来像实盘的路径”，而追求四种一致性：

1. **Protocol equivalence**：同一输入下，过滤器、订单状态机、费用、资金费、保证金和错误处理与交易所公开契约一致；
2. **Observation equivalence**：模拟器只使用实盘可在同一时刻获得的数据，并复制聚合、删失、延迟、乱序和缺失；
3. **Distributional equivalence**：在相同 context 下，成交、markout、延迟、滑点和风险事件分布与实盘误差有界；
4. **Decision equivalence**：同一策略在 simulation/shadow/微额实盘中的动作、风险判断和策略排序稳定。

只要公开数据不能识别撮合内部状态，就不能宣称 exact。真值层级固定为：

exchange/account fact > raw received packet > reconstructed state > set-valued hidden state > generative stress。

低层模型不得覆盖高层事实。S2 输出必须带 truth_level、不确定区间和校准版本。

历史路径无法响应本策略的反事实订单。Factual Replay 同时输出 no-impact 主路径和 conservative reactive-impact overlay；策略必须在两者及生成压力族中都成立。以 1 万人民币规模可假设影响较小，但不能假设严格为零。

## 38. S2 单一离散事件内核与多时钟模型

所有 Simulation/Replay/Testnet/Live adapter 共享同一事件调度器和状态转移函数。每个事件保存：

- exchange event time、transaction time、output time；
- local wall-clock receive/send time；
- local monotonic receive/decision/send/ack time；
- server-clock offset 区间与测量误差；
- stream/connection/sequence/attempt 标识。

不同 WebSocket、REST 和账户流只形成因果偏序，禁止按本机到达时间伪造唯一“交易所总顺序”。决策快照只能读取其 happens-before closure。

延迟向量联合建模：
$L=(L_{md},L_{decision},L_{out},L_{engine},L_{ack},L_{cancel},L_{reconcile})$。

不独立采样各段延迟。按连接、时段、负载和故障状态拟合经验条件分布；中心体用 empirical copula，尾部用 peaks-over-threshold/极值压力包。重连风暴、DNS/代理切换、磁盘反压和 CPU 抖动进入共同 operational regime。

recvWindow 在撮合到达时按 server clock 校验；本地超时不等于订单失败。503/timeout/unknown response 必须进入 UNKNOWN_PENDING_RECONCILE，先查 user stream/order query，禁止直接生成重复订单。

## 39. 行情观测模型：复制交易所给我们的世界

本地订单簿严格执行 Binance 的 snapshot + diff contract：

1. 先缓冲 diff；
2. 获取 snapshot；
3. 丢弃过旧更新；
4. 首事件满足 $U\le lastUpdateId\le u$；
5. 后续必须 $pu=previous_u$；
6. 档位数量是绝对量，0 表示删除；
7. gap 立即冻结并重新同步。

每次 snapshot/resync 记录原始包 hash、覆盖区间、缓冲事件数、丢弃原因和 book generation。策略不得跨 generation 使用特征或队列状态。

公开 depth 有限且不包含 RPI 隐藏订单；aggTrade 是聚合观测；强平流也可能是时间窗快照。因此观测方程写成：

$Y_t=\mathcal C_t(X_t)+\varepsilon_t$，

其中 $\mathcal C_t$ 显式表示深度截断、事件聚合、隐藏流动性和流删失。模拟器不得把 $Y_t$ 当完整撮合状态 $X_t$。

信号价格、交易价格、风险价格继续分离：

- Index residual：相对固定收盘锚点的经济偏离；
- Contract bid/ask/mid：下单和真实成交；
- Mark：未实现盈亏、保证金和清算；
- FX：人民币初始资金与报告换算，不能混入 USDT 交易 PnL。

A/HK 交易日历、临时休市、半日市、交易所 tradingSchedule、距开盘时间和锚点来源都作为版本化输入。日历未知或锚点 hash 改变时只能降低风险。

## 40. 撮合与订单生命周期数字孪生

订单状态机至少覆盖：

LOCAL_CREATED → SENT → ARRIVED_UNKNOWN/REJECTED/NEW → PARTIALLY_FILLED → FILLED/CANCELED/EXPIRED/EXPIRED_IN_MATCH/CALCULATED。

Modify/Cancel 是独立命令和竞态，不得原地改对象掩盖中间状态。ORDER_TRADE_UPDATE 中 NEW、TRADE、AMENDMENT、CANCELED、EXPIRED 和 CALCULATED 分别落事件；expiry reason、STP、强平、ADL、下市和 reduce-only 冲突不可合并为普通取消。

每次命令使用稳定 intentId、唯一 clientOrderId、attemptId 和可关联 modifyId。UNKNOWN 后重试必须证明前一次未执行；否则只能 query/reconcile。REST response、WebSocket order update 和账户变化三方不一致时按字段权威源处理：订单状态以 order update/query 为准，成交以 trade/fill 为准，余额仓位以 account update/query 为准；任何冲突都产生 reconciliation incident，禁止一种流覆盖另一种流不负责的字段。

Post-only/GTX 在模拟的 exchange-arrival book 上判定，不在 decision-time book 判定。撮合、ACK、user-stream 到达允许不同顺序。部分成交后撤单、撤单 ACK 前成交、重复 update 和迟到 update 都必须可复放。

## 41. 队列部分识别与成交边界

公开 price-level depth 没有订单 ID，且存在隐藏 RPI 流动性，因此自己的绝对排队位置不可观测。对每笔 maker order 维护：

$Q^{ahead}_t\in[Q^-_t,Q^+_t],\quad
P(fill\le h)\in[p^-_h,p^+_h]$。

同价位成交量确定减少队前量；同价位净撤单按“撤在我前方的比例”$\eta_t\in[\eta^-,\eta^+]$传播上下界；新增量默认在已挂订单之后，除非交易所语义或实盘证据说明其他优先级。

价格修改、减量修改、cancel-replace 是否保留优先级必须按 USDⓈ-M 当前接口逐项校准，不能套用 Spot 的 amend-keep-priority 规则。文档没有明确保证时按丢失优先级处理。

每笔成交同时产出 strict/pessimistic、posterior/base、optimistic 三条账本。主结论要求 strict 路径通过；base 用于效率估计；optimistic 只显示识别宽度，不能用于升级。

成交模型采用 competing risks：

$\lambda=(\lambda_{fill},\lambda_{adverseMove},\lambda_{cancelAck},\lambda_{expire},\lambda_{disconnect})$。

fill probability、time-to-fill 和 fill 后 markout 联合估计。校准目标不是提高成交率，而是正确预测“成交且随后不利移动”的联合概率。

对于市价/紧急减仓，按到达时可见深度逐档 sweep，并加隐藏流动性/延迟后的 depletion envelope。超出可见深度部分使用保守 impact curve，不允许按最后一档无限成交。

## 42. 账户、资金与交易所风险真值

统一账户状态必须包含 wallet balance、available balance、cross/isolated、position side、entry/break-even/mark、initial/maintenance margin、leverage bracket、open-order margin、fee tier、BNB fee setting、regular/special funding、insurance/ADL risk。

逐事件不变量：

$Equity=WalletBalance+UnrealizedPnL$，

$\Delta Wallet=RealizedPnL-Commission+Funding+Transfer+Adjustment$。

强平价不能用常数杠杆近似，必须根据当前 bracket、maintenance margin、未成交订单和账户模式重算。跨标的共享保证金时，对一个标的的冲击必须传播到全部仓位。

资金费按实际结算资格、position notional、rateType 和账户事件入账；Regular 与股票分红产生的 Special 永久分列。预测 funding 只用于决策，实际 ledger 只能用交易所结算事实。

限流是状态而非异常字符串：REQUEST_WEIGHT、ORDERS、连接握手、ping/pong、429、-1008 分别建模。风险减仓享受的交易所优先/豁免只按官方语义触发，普通下单不能冒充 reduce-only。

## 43. S2 校准：数字孪生误差模型

对每类可观测结果定义：

$Y^{live}=Y^{sim}_\theta+\delta_\phi(context,action)+\epsilon$。

$\delta_\phi$ 是 simulator-to-live discrepancy，不强迫其为零。按 symbol、session phase、side、queue bucket、volatility、latency regime 分层，用层级模型收缩；数据不足时区间自动变宽。

校准数据依次来自 factual replay、只读 shadow、Testnet 协议验证和未来经授权的极小额度探针。Testnet 只能验证协议和状态机，不能代替主网流动性/成交校准。任何真实下单阶段都必须单独授权，本轮不启动。

S2 不是单模型，而是模型集合：
$\mathfrak S=\{S_{strict},S_{posterior},S_{stress,1},\ldots,S_{stress,K}\}$。

策略升级要求在整个可信集合上的风险约束成立。某个生成器即使边际分布相似，只要产生不真实的条件响应、策略影响或策略排名，就从可信集合移除。

生成模型校准与策略评测严格数据隔离；禁止用“让 M7 收益更高”作为模拟器损失。模拟器只拟合实盘事实和预注册真实性统计。

## 44. M7 联合状态空间与不确定性分解

固定锚点 $A_i$ 不变。每个标的的经济、执行和风险状态分别为：

$x_i=\log(Index_i/A_i)$，
$b_i=\log(Mid_i/Index_i)$，
$m_i=\log(Mark_i/Index_i)$。

$x_i$ 使用 semi-Markov jump-OU；$b_i,m_i$ 使用状态依赖短记忆过程并允许流动性冲击时跳变。这样收敛收益、contract-index basis 和清算 basis 不再混为同一均值回归。

组合共同因子：
$dx=B_s f_tdt+D_s dW_t+dJ_t+\varepsilon_t$。

$B_s$ 采用分层稀疏先验，避免 7 标的小样本协方差爆炸。共同跳跃、A/HK 市场类别、闭盘阶段和临近开盘风险显式进入状态。

不确定性集合定义为：
$\mathcal U_t=\{Q:Q_m\in\mathcal U^{market}_t,\ Q_e\in\mathcal U^{execution}_t,\ Q_o\in\mathcal U^{operation}_t,\ Q\in\Gamma^{causal}_t\}$，
其中 $\Gamma^{causal}_t$ 保留时间和市场—执行—系统故障的联合依赖。不能分别取三个温和边界后再假设它们独立或同时温和。

每个候选动作同时输出 expected、posterior lower bound、DRO worst case 和 named stress loss。任何一个硬生存约束失败即删除候选，不允许收益打分补偿。

## 45. Safety Shield：鲁棒可生存域

定义安全余量：

$h_{margin}=Equity^{stress}-MaintenanceMargin-Reserve$，

以及 position、gross、net、concentration、data age、order uncertainty、time-to-open 等 $h_j(z)$。

动作 $a$ 仅在以下条件成立时可进入优化器：

$\inf_{w\in\mathcal W_t}h_j(F(z,a,w))\ge(1-\gamma_j)h_j(z),\quad\forall j$，

并且从所有后继状态仍存在一条可执行的 emergency reduce/flatten 路径。该离散 barrier/viability 条件保证新增风险不会把系统推进“已经来不及退出”的状态。

低维单标的边界用 Hamilton-Jacobi reachability 或稠密动态规划离线核验；7 标的在线采用可分解保守 barrier、stress polytope 和 margin oracle。高维近似必须偏保守，不能用未验证神经 barrier。

压力集至少含：锚点暂时失效、单边跳跃、跨标的共同跳跃、mark-index 脱钩、盘口骤空、取消失败、ACK 丢失、延迟尾部、funding/special funding、临近开盘、规则变化、API throttle 和进程恢复。

绝对无限跳跃无法保证存活。安全证书必须显示 threat-set version、最坏路径、最小余量和证书覆盖范围。

## 46. M7 词典序鲁棒控制问题

第一层求最大可生存规模：
$q^{safe}=\sup\{q:a(q)\in\mathcal A_{viable}(z_t)\}$。

第二层只在 $[0,q^{safe}]$ 内求：

$\max_{\pi}\inf_{Q\in\mathcal U_t}
E_Q[\sum_h\Delta\log W_h-c_{exec}-c_{churn}]
-\lambda CVaR^Q_\alpha(L)$。

第三层用 tie-breaker 依次最小化 max drawdown proxy、换手、模型敏感度和策略复杂度。这样“好市场强收益”只能来自安全域扩大和更高净机会质量，不能来自放松生存约束。

候选动作价值必须包括：

- 首达目标前先触及 stop/broken/open 的概率；
- maker fill 与 adverse markout 的联合分布；
- cancel/replace 丢失队列的机会成本；
- funding、special funding、fee tier 与最终清仓；
- 当前持仓和所有未决订单的联合最坏暴露；
- NO_ACTION 的机会成本与保留未来选择权的价值。

M7 采用 belief-space receding horizon。远期价值用保守 terminal value approximation；离开训练/校准支持集时 terminal value 归零或变负，禁止乐观外推。

## 47. 求解器与最优性证书

在线求解流程：

1. 更新半马尔可夫/BOCPD belief 和模型权重；
2. Safety Shield 枚举删除不可行首动作；
3. 对剩余动作使用共同随机数生成条件场景树；
4. 用 causal-Wasserstein 对偶或 column-and-constraint generation 求最坏分布；
5. 7 标的仓位子问题用 convex CVaR/log-growth program；
6. tick/lot/min-notional 离散化后做可行性修复；
7. 返回最优首动作与完整证书。

每次求解记录 lower bound、upper bound、duality/optimality gap、约束 slack、最坏场景、活跃约束、迭代数、耗时和 fallback reason。

实时预算到期时采用 incumbent feasible action，不采用未证可行的高价值动作。无可行 incumbent 时只允许 CANCEL、REDUCE_ONLY、FLATTEN、HALT。

离线用高精度动态规划/PIDE、nested Monte Carlo 和独立求解器交叉验证。在线近似相对离线 oracle 的 regret 必须分 context 报告；oracle 使用未来信息时只能称 upper bound，不能作为可交易基线。

## 48. 日志：从记录结果升级为因果事件溯源

日志分为五条不可混写的数据平面：

1. Raw Wire Journal：收到/发出的原始 bytes、headers、endpoint、connection、压缩和解析状态；
2. Canonical Domain Events：规范化 market/order/account/system 事件；
3. Decision Journal：特征、belief、候选、约束、价值和最终动作；
4. Economic Ledger：逐 fill/fee/funding/transfer 的双式记账与 PnL 分解；
5. Metric/Report Store：可重算派生结果，不是真值源。

每条记录统一包含 schemaVersion、runId、ledgerId、strategyId、simulatorId、eventId、parentId、traceId、streamId、seq、所有时钟、entityId、truthLevel、payloadHash、codeVersion、configHash、modelVersion 和 uncertainty。

Raw → Canonical → State → Decision → Intent → Request → ExchangeEvent → Fill → Ledger → Metric 形成可查询 DAG。跨流只记录已知因果边；未知顺序保持并发，不能随意排序。

任一 NO_ACTION 必须保存完整 candidate set、首个失败硬约束、所有 slack、LCB、NO_ACTION value 和模型支持度。任一风险减仓必须区分 target reduction、emergency reduction、pre-open、funding、margin、data failure 和 operator action。

## 49. 日志可靠性与可恢复性

真值与账本 writer 禁止 try-send 丢弃。采用有界 write-ahead queue：

- 到高水位：停止产生新增风险；
- 到临界位：撤单并进入 risk-reducing only；
- fsync/磁盘失败：HALT_NEW_RISK，保留内存紧急事件并报警；
- 恢复后从 checkpoint + hash chain 重放。

处理语义采用 at-least-once ingestion + idempotent state transition，不伪称分布式 exactly-once。每个原始包和交易所事件以稳定键去重，重复本身仍作为 telemetry 记录。

文件采用按大小/时间分段的 append-only journal，临时文件原子 rename，段尾 checksum、前后 hash chain、事件计数和最小/最大时间。checkpoint 保存账本、订单状态、book generation、过滤器 belief、随机数状态和最后消费 offset。

run manifest 额外固定：git commit、dirty patch hash、GNU toolchain、binary/config/schema/data hash、OS/CPU、timezone、dependency lock、seed tree、交易所文档版本、合约规格、费率、账户模式、启动/退出来源和退出码。

敏感凭证永不进入日志；请求只保存安全指纹和业务参数。日志 schema 向后兼容 reader，迁移必须保留原始段，不能覆盖旧实验。

## 50. 决策证书与事故包

每次动作额外生成 DecisionCertificate：

$C_t=(I_t,B_t,\mathcal A_t,\mathcal U_t,\mathcal W_t,
V^-,V^+,slack,gap,a^*,fallback)$。

其中 $I_t$ 是可见信息闭包，$B_t$ 是 belief，$\mathcal A_t$ 是候选集，$\mathcal U_t/\mathcal W_t$ 是模型与压力集合。证书能够回答“为什么交易、为什么这个量、为什么这个价格、为什么当时没退出”。

每次 fill 保存 queue interval、arrival book、maker/taker、commission、mark/index/mid、100ms/1s/5s/30s/terminal markout、对应候选预测和随后 realized outcome。

Incident bundle 自动触发条件包括：大亏、硬约束逼近、unknown order、reconciliation mismatch、writer backpressure、book resync、clock jump、solver failure、异常 funding、强平/ADL、进程非正常退出。

事故包包含事件前后窗口、原始包、DAG 子图、状态快照、订单时间线、账本、solver certificate、线程/资源指标和重放命令。任何事故必须能在隔离环境确定性重演到同一 hash。

## 51. 指标：模拟器真实性认证

真实性不能压缩成一个可被优化欺骗的总分。四张独立证书分别通过：

### 51.1 Protocol Conformance

- snapshot/diff/resync、订单状态转移、重复/乱序幂等；
- filter、tick/step/min-notional、GTX、reduce-only、STP、expiry；
- fee/funding/margin/liquidation/ADL；
- recvWindow、timeout UNKNOWN、429/-1008、listenKey expiry；
- checkpoint/recovery 后状态一致。

协议项要求 100% 通过；不存在“平均 99.9% 就够”。

### 51.2 Observation Fidelity

按 symbol × session phase × regime 比较真实与模拟：

- inter-arrival、event type、size、spread、depth、imbalance；
- return/volatility/vol-of-vol、tail index、jump、duration；
- order-flow autocorrelation、cross-level/cross-side dependence；
- mark-index/contract-index basis、funding、流动性恢复时间；
- gap、重复、乱序、重连和延迟联合分布。

报告 Wasserstein、energy distance、MMD、conditional classifier two-sample AUC、quantile error 和 tail exceedance error。所有特征先用真实训练窗尺度标准化，不能让高方差变量支配距离。

### 51.3 Execution Fidelity

- fill probability：Brier、log loss、reliability curve、ECE；
- time-to-fill/cancel：survival calibration、integrated Brier、censoring-aware concordance；
- queue interval：coverage、平均宽度、conditional coverage；
- fill 后 markout：均值、分位数、expected shortfall、方向/状态条件误差；
- market/urgent order：implementation shortfall、depth sweep、尾部滑点；
- ACK/cancel/reconcile latency：联合分位数、copula/tail dependence；
- post-only reject、partial fill、cancel race、STP、expired reason 频率。

预测区间只有“覆盖率高且宽度足够窄”才合格。用无限宽区间达到覆盖率视为失败。

### 51.4 Decision Fidelity

在相同 raw prefix 上比较 simulation/shadow/live：

- feature/belief/action 一致率；
- risk state 与 first-failed-constraint 一致率；
- order intent 到交易所状态的转移混淆矩阵；
- predicted 与 realized PnL component；
- 各方法策略排序的 Kendall/Spearman、top-k stability；
- 模拟器参数扰动下 pairwise delta 符号稳定率。

策略排名不稳定时，模拟器不得用于选择 M7，即使单项 stylized facts 很漂亮。

## 52. 策略与生存指标的最终分层

Primary survival：

- liquidation/ADL/ruin upper confidence bound；
- stress 后最小 margin buffer；
- max drawdown、duration、time-under-water、recovery；
- CVaR/expected shortfall、worst excursion、overshoot area；
- emergency exit completion probability/time/cost。

Primary economic：

- exact net PnL 与 return on allocated capital；
- net log-growth、Calmar、Omega、Ulcer；
- PnL per gross-notional-hour、margin-hour 和 opportunity；
- convergence capture ratio、good-market capture、bad-market avoidance；
- fee/funding/adverse-selection/emergency/rebalance drag。

Primary decision：

- canonical opportunity recall/precision；
- $p_{conv}$、edge、tail loss 与 regime posterior calibration；
- false mean-reversion entry、late exit、missed safe opportunity；
- realized regret 对可交易基线；clairvoyant 仅列 upper bound；
- NO_ACTION 的避免损失和机会成本。

Primary execution：

- maker/taker 比例、有效/已实现 spread；
- queue-adjusted fill、partial ratio、time-to-fill；
- 100ms/1s/5s/30s/terminal markout；
- quote age、cancel-to-fill、reprice churn；
- reduce-only schedule 的完成率、超时率和成本。

Primary portfolio：

- target/actual tracking、hard-cap overshoot area；
- gross/net/factor exposure、HHI、marginal CVaR；
- risk-budget utilization、rebalance turnover；
- shock propagation、共同跳跃损失与分散化失效；
- M6→M7 动态资金的配对增量。

## 53. 日志与运行可靠性指标

- raw/canonical/ledger event completeness；
- unknown parent、broken trace、orphan fill、duplicate/conflict；
- writer queue high-water、backpressure duration、fsync latency；
- checkpoint age、recovery time、replay hash mismatch；
- book generation count、resync duration、stale exposure；
- clock offset/uncertainty、negative latency、causal-order violation；
- reconciliation count、金额、持续时间、unexplained PnL；
- CPU/memory/network/disk、decision/solver p50/p95/p99/max；
- graceful/forced/crash exit 与最后 durable event。

硬指标：账本事件 drop=0、unexplained PnL=0、orphan fill=0、硬约束穿透=0、replay hash mismatch=0。任一非零则该 run 不具备策略比较资格。

## 54. 无限运行下的统计推断

“时间无限”意味着允许持续查看结果，普通固定样本 p-value 会失效。统计单位仍为 closed session、独立 anchor excursion 和预注册 stress seed，不是 tick 或 30 秒 PnL。

同时维护：

1. 固定冻结 horizon 的 block bootstrap/HAC/SPA/DSR；
2. session-block 的 anytime-valid confidence sequence/e-process；
3. M1–M7 同一事件流与 seed 的 paired delta；
4. symbol/session/regime 的层级效应与 leave-one-symbol-out 稳健性。

连续查看只能依据预先定义的 confidence sequence；不能看到好结果就停。正式升级仍需独立锁箱窗口，且置信下界超过最小经济效应。

对每个增量 $M_k-M_{k-1}$ 报告 mean/median/Hodges-Lehmann、paired win rate、block-bootstrap CI、$P(\Delta>0)$、风险非劣概率和贡献集中度。多版本搜索继续用 White Reality Check/Hansen SPA 与 Deflated Sharpe。

## 55. 真实性与策略升级门禁

Gate 0 — Specification：交易所契约、公式、schema、threat set、主指标和阈值冻结。

Gate 1 — Deterministic Conformance：golden packets、property tests、状态机 model checking、故障注入和重放 hash 全通过。

Gate 2 — Historical Factual Replay：全部 7 标的跨 session 的 protocol/observation 指标通过；strict queue 和 impact overlay 下生存。

Gate 3 — Shadow：使用主网只读行情/账户语义，策略产生意图但不发订单；检查 decision、延迟、限流和运行可靠性。

Gate 4 — Testnet：只验证认证、订单生命周期、错误、恢复和对账，不用于证明成交收益。

Gate 5 — Micro Live：只有经用户单独授权才运行；极小额度、硬损失上限、自动 kill、逐订单探针，用于 fill/latency/markout 和 simulator discrepancy 校准。

Gate 6 — Limited Production：M7 必须在独立锁箱、strict simulator family、shadow/micro-live 证据中同时通过，且相对 M6 收益显著、风险非劣。

任何 Gate 失败都回到对应模型层，不允许通过调策略掩盖模拟器失败。实盘证据逐级增加权限，不一次性跳级。

## 56. Round 5 实施切片

P0：完成已有 L1——reduce-only 方向、lifetime 指标、reject taxonomy、manifest/exit/writer 错误。

P1：Raw Wire Journal、多时钟、因果偏序、统一 Envelope、无丢失账本。

P2：Binance conformance——book generation、UNKNOWN reconcile、完整订单/账户/资金费/限流状态机。

P3：S2 factual queue interval、strict/base/optimistic ledger、market-order depth sweep 和 impact overlay。

P4：真实性 metric suite、条件校准、simulator discrepancy 与认证报告。

P5：M7 belief、首达 PIDE、execution competing risks、Safety Shield。

P6：causal-DRO MPC、7 标的组合优化、求解证书与 fail-safe。

P7：完整 M1–M7 × 7 标的 × F/R 公共事件流实验，生成每方法每标的全部报告。

Round 5 仍是设计轮，未修改 engine 代码、未编译、未启动实验。下一实际动作应从 P0 开始，不跨过日志与协议基础直接实现 M7。

## 57. Round 5 新增研究与官方依据

- [Binance：正确维护 USDⓈ-M 本地订单簿](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)：规定 snapshot/diff、U/u/pu 和 gap 重同步语义。
- [Binance USDⓈ-M Market Data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/market-data)：公开深度有限且不含 RPI 订单，因此队列必须部分识别。
- [Binance USDⓈ-M General Info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)：timeout/503 可能为执行状态未知、recvWindow 与 -1008 风险减仓语义。
- [Binance USDⓈ-M User Data Streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams)：订单、AMENDMENT、STP expiry、强平/ADL、账户和 listenKey 事件。
- [Binance USDⓈ-M Change Log](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log)：TradFi tradingSchedule、Special funding 与接口变化必须版本化。
- [Get Real：LOB 模拟器真实性指标](https://arxiv.org/abs/1912.04941)：支持以多类 stylized facts 和真实性差距认证模拟器。
- [LOB-Bench](https://arxiv.org/abs/2502.09172)：支持条件化、多维和策略响应层面的生成式 LOB 评估。
- [Limit Order Book Simulations Review](https://arxiv.org/abs/2402.17359)：用于选择 Queue-Reactive、Hawkes、agent-based 与生成模型的适用边界。
- [Optimal Execution under Incomplete Information](https://arxiv.org/abs/2411.04616)：支持部分信息、marked Hawkes 与冲击下的执行控制。
- [Causal/Adapted Wasserstein DRO](https://arxiv.org/abs/2401.16556)：用于不使用未来信息的动态分布鲁棒优化。
- [Safe Anytime-Valid Inference](https://arxiv.org/abs/2210.01948)：用于无限运行和持续查看结果时的置信序列/e-process。

## 58. Round 6 目标：把核心机制变成可证伪假设

核心假设不是“策略曾经盈利”，而是：在 A/HK 标的闭盘、锚点有效且尚未重新开盘的条件下，Binance TradFi 永续指数相对官方固定收盘锚点的残差，存在可重复、可在剩余闭市时间内兑现、扣除可执行成本后仍有价值的条件均值回归。

本轮只新增所有 M1–Mxx 共用的 ValidationClaim Verification Layer，不新增 M8，不改变任何历史版本。结构性市场假设、执行可交易性和具体策略收益必须分开验证。

证据链分为五层：

1. H-data：锚点、映射、FX、公司行动、时间和行情足以定义无歧义残差；
2. H-drift：偏离后条件漂移方向朝向固定锚点；
3. H-center：长期中心在预注册容差内等价于零，而非仅仅“无法拒绝不为零”；
4. H-deadline：在开盘、风险止损或数据失效前，触达目标带的概率和时间具有实用价值；
5. H-economic：按严格成交模拟扣除手续费、滑点、资金费和冲击后，保守净边际仍为正。

任何一层失败都必须如实标记；不得用后一层偶然盈利掩盖前一层失败，也不得由策略筛选后的成交样本反推市场规律。

## 59. 固定锚点、残差与无选择偏差样本

对标的 i、闭市 session s，使用在闭市开始前已确定并冻结、且已映射到合约报价单位的有效官方收盘锚点 A(i,s)。原始股票收盘价固定不变；若合约规格要求币种/比例转换，则转换规则与所用 FX reference 也必须在 session 前冻结。锚点须携带来源、发布时间、币种、FX 版本、合约映射、除权除息/公司行动和内容哈希。任何未知调整不得按 0 处理；迟到修正使该 session 失效，不可静默回写历史。

主结构残差定义为：

$$x_{i,s,t}=\log(I_{i,t}/A_{i,s}),$$

其中 I 是 Binance 指数价格。合约 mid、mark 与可成交 bid/ask 的残差分别记录，只用于基差、清算与经济层，不能污染结构层对指数锚定机制的检验。

建立 Canonical ValidationClaim Opportunity 流：在预注册时钟网格或一次独立 anchor excursion 首次穿越残差桶边界时生成样本，不取决于任何 M 版本是否下单。所有方法引用同一个 evidence_id；NO_ACTION 也有完整结果。

主统计单位是 completed closed session 或相互分离的 anchor excursion，不是 tick。预注册 1、5、15、30、60 分钟及 terminal/open-deadline horizon；重叠 horizon 使用 session block/HAC，主确认集优先采用非重叠 excursion。

样本切分永久冻结为 discovery、calibration、lockbox。模型、阈值和残差桶只能在前两者调整；lockbox 只运行一次正式确认。

## 60. 方向收缩、动态形状与锚点中心检验

对每个 horizon h，同时计算：

$$D_h=-\operatorname{sign}(x_t)(x_{t+h}-x_t),\quad C_h=|x_t|-|x_{t+h}|,$$

$$R_h=\log\frac{|x_t|+\epsilon}{|x_{t+h}|+\epsilon},\quad Q_h=1\{|x_{t+h}|<|x_t|\}.$$

D、C、R 为正和 Q 大于预注册基准才表示朝锚点收缩；结果按标的、方向、初始残差桶、闭市阶段、波动/流动性/消息 regime 报告。

主条件局部投影：

$$x_{t+h}-x_t=\alpha_h+\beta_h x_t+\gamma_h^\top Z_t+u_{i,s,t,h},$$

其中 Z 只能含 t 时刻可得信息，并加入 symbol/session/calendar 固定效应。核心方向证据为关键 horizon 上 beta_h 的置信上界小于 0，并满足最小经济效应，而不只是 p<0.05。

再拟合支持不规则时间与跳跃的状态条件模型：

$$x_{t+\Delta}=\mu_z+(x_t-\mu_z)e^{-\kappa_z\Delta}+\varepsilon_{t,\Delta}.$$

必须联合证明 kappa_z 大于预注册最小速度，并对 |mu_z|<epsilon_anchor 做 TOST/等价性检验。未拒绝 mu=0 不构成锚点正确证据；epsilon_anchor 由报价精度、锚点误差和最低可交易边际事先确定。

另估计非参数漂移 g_z(x)=E[Delta x/Delta t | x,Z=z] 及同时置信带；在可交易残差区要求 x*g_z(x)<0，用于暴露非线性、阈值效应和多空不对称。ADF/PP/KPSS 只作辅助诊断，不能单独通过核心假设。

## 61. 开盘截止首达与经济可交易性

定义竞争风险：tau_target 为首次进入锚点目标带，tau_adverse 为首次触发预注册风险带，tau_open 为开盘/强制退出截止，tau_invalid 为锚点或数据失效。

估计条件累计发生概率：

$$p^*(x,z)=P(\tau_{target}<\tau_{adverse}\wedge\tau_{open}\wedge\tau_{invalid}\mid x,z),$$

并报告 restricted mean time-to-target、未到达比例、目标前最大不利偏离和右删失原因。延迟 horizon 尚未成熟时保持 pending/right-censored，绝不能记作“未回归”。

结构层通过后，另建立与任何 M 无关的标准化虚拟交易探针：在相同 opportunity 上按 S2 strict ledger 的真实 bid/ask、队列区间、延迟、手续费、资金费、冲击和强制退出规则执行。

净实现价值为：

$$G_h^{net}=G_h^{anchor}-fee-slippage-funding-impact-risk\ buffer.$$

H-economic 要求 session-block anytime 置信下界高于预注册最小净边际，同时 target-before-adverse 概率下界超过门槛。结构有效但净边际不正时结论必须是“统计回归、当前不可交易”，不得宣称策略有效。

策略层仍单独比较 M1–Mxx 的真实账本收益、风险和机会成本；它回答“谁利用得更好”，不负责证明“现象存在”。

## 62. 反证、安慰剂与替代解释排除

每个正式样本同步计算、但不用于交易的安慰剂锚点：同标的同日历 regime 的日期置换收盘、尺度匹配的跨标的收盘、与残差分布匹配的合成锚点、以及预注册的 close 前时间点价格。使用未来收盘的安慰剂仅可离线标记，严禁进入特征或决策。

真实官方收盘锚点必须在配对 session 统计上显著优于安慰剂，而不只是自身 beta<0。否则可能只是一般价格负自相关、波动衰减或构造性机械回归。

必须额外报告：

- 开市期间相同时间长度的负对照；
- 距官方收盘远近与收缩强度的剂量响应；
- 控制 BTC/美股指数、FX、整体 TradFi 因子后的残差漂移；
- 重大消息、跳跃、流动性坍塌和开盘邻近的交互项；
- leave-one-date-out、leave-one-symbol-out、单边方向剔除和极端日剔除。

若证据只存在于单一标的、单一方向、少数日期或某个残差桶，结论降级为 conditional support，并只允许策略在证据支持的 cell 中增加风险；不可外推为全市场规律。

## 63. 无限运行、多重检验与防止 p-hacking

持续实验使用 completed-session block 的 anytime-valid confidence sequence/e-process；普通固定样本 p-value 只在冻结 horizon 报告。任何人在任意时刻查看结果都不得改变错误率。

primary family 预注册为少量关键 horizon、7 个正式标的和两方向，采用层级 closed testing/Holm 控制 FWER；探索性 regime/残差桶使用 BH 或 e-value FDR，并明确标注 exploratory。

版本、阈值和模型搜索不共享 lockbox。模型比较采用 prequential 一步向前评分；正式结论需在后续未见 session 上复制。所有停止、排除、锚点修正和缺失规则写入 manifest。

## 64. 假设日志、逐股指标与证据面板

新增与 Method ledger 解耦的 ValidationClaimOpportunity：evidence_id、symbol/session、anchor 全谱系、t0、x0、方向/桶/regime、可观测控制量、数据有效性、各 horizon due time。

新增 ValidationOutcome：x_h、D/C/R/Q、target/adverse/open/invalid 时间与顺序、删失原因、最大有利/不利偏离、strict/base 执行结果、完整成本分解、暴露中的数据 gap。记录生成后不可覆盖，只能追加 correction/invalidation event。

每个标的和组合层至少输出：

- horizon contraction curve 与 simultaneous confidence band；
- beta_h 局部投影曲线、非参数 drift shape 和多空不对称；
- kappa、half-life、mu 及 anchor-equivalence 区间；
- target/adverse/open competing-risk CIF 与剩余闭市时间分层；
- 官方锚点相对全部 placebo 的配对优势和 rank；
- gross-to-net edge waterfall、严格队列上下界与成本敏感度；
- symbol × direction × residual bucket × session phase × regime 证据热图；
- anytime evidence trajectory、有效样本量、删失率和 leave-one-out 稳健性。

汇总报告必须同时给 micro average、symbol-equal macro average、最弱标的、最坏 regime 和贡献集中度，防止大样本标的掩盖失败标的。每个 M1–Mxx 继续获得逐方法逐标的完整指标，但核心假设面板只计算一次并由全部方法共享。

## 65. 预注册判决与策略使用规则

每个 symbol × direction × residual bucket × session phase × regime cell 输出四态判决：SUPPORTED、CONDITIONALLY_SUPPORTED、INCONCLUSIVE、REJECTED。

SUPPORTED 至少要求同时满足：数据层通过；关键 horizon 的 beta_h 和 D/C 下界满足最小效应；mu 通过锚点等价性；target-before-adverse/open 下界达标；真实锚点显著优于 placebo；strict 净边际下界为正；证据不由单日支配。

阈值不得从同一测试集反复调到通过。epsilon_anchor、最小收缩、最低首达概率和最低净边际由锚点误差、总摩擦、风险预算和开盘前剩余时间推导并在 manifest 冻结。

策略可将判决和实时置信下界作为 eligibility/risk multiplier：SUPPORTED 正常使用，CONDITIONALLY_SUPPORTED 降额且限定 cell，INCONCLUSIVE 只记录不增险，REJECTED 禁止该机制开仓。Safety Shield 始终具有更高优先级。

该门控属于可变策略层；“官方收盘是候选固定锚点、必须接受独立证伪”属于不可变研究原则。若核心假设整体被拒绝，应停止新增版本优化，而不是继续拟合收益。

## 66. Round 6 实施切片

H0：冻结 evidence manifest、样本单位、horizon、最小效应、placebo 和多重检验 family。
H1：实现独立 opportunity/outcome 事件、锚点谱系与 right-censor completion worker。
H2：实现局部投影、TOST、跳跃 OU、非参数漂移与 competing-risk 分析。
H3：接入 S2 strict 标准化交易探针和 gross-to-net 经济层。
H4：生成逐股证据面板、anytime evidence、placebo 与 leave-one-out 报告。
H5：完成 discovery/calibration 后冻结；只在新 lockbox session 作正式判决，再供 M1–Mxx eligibility 使用。

Round 6 仍是设计轮，未修改 engine 代码、未编译、未启动实验。实际实施仍须先完成 Round 5 的 P0–P3 数据、日志和 strict simulator 基础，再并行推进 H0–H4。

## 67. Round 6 方法依据

- Dickey–Fuller 单位根检验用于辅助识别持久性，但不等价于证明固定锚点正确。
- KPSS 以平稳性为原假设，可与单位根检验互补；结构突变、跳跃与短 session 下仍仅作诊断。
- TOST/等价性检验用于主动证明长期中心位于预注册锚点容差内，避免把“不显著”误读为“相等”。
- Jordà local projections 用于直接估计不同 horizon 的状态条件收缩路径，减少错误指定单一动态模型的依赖。
- competing risks/survival 处理 target、adverse、open deadline 与右删失，回答回归是否及时。
- Safe anytime-valid inference/confidence sequences 用于无限运行和持续查看，session block 保留序列依赖。

主要文献：Dickey & Fuller (1979), Distribution of the Estimators for Autoregressive Time Series With a Unit Root；Kwiatkowski et al. (1992), Testing the Null ValidationClaim of Stationarity；Jordà (2005), Estimation and Inference of Impulse Responses by Local Projections；Lakens (2017), Equivalence Tests；Safe Anytime-Valid Inference (2023)。

## 68. Round 7 核心结论：从高拟合升级为可识别、可校准、可证书化

Round 7 不创建 M8。S2、M7 和 Round 6 假设层的方向保留，但补齐三个缺口：

1. observed residual 的收缩不等于固定锚点机制，必须排除共同因子、机械负相关、测量误差和一般波动衰减；
2. 单一“最像历史”的模拟器不能代表实盘，必须给模拟器误差建立有限样本可信集合并让策略在集合内生存；
3. 静态 CVaR、平均 PnL 和普通 Monte Carlo 不能可靠约束动态回撤与罕见破产，必须采用时间一致风险递归和稀有事件估计。

因此“最优解”严格定义为：在冻结信息集、动作网格、可信模型集合、风险容忍度和计算时限内，通过安全可行性、取得可验证 primal/dual gap 的最优策略。未知威胁集之外不承诺全局最优或绝对存活。

不可变：官方收盘候选锚点固定、先证伪后使用、硬安全优先、M1–Mxx 公共事件流并行、模拟器与策略隔离。可变：模型权重、置信半径、风险预算、动作规模、执行方式和 evidence cell eligibility。

## 69. 锚点机制的结构分解与可识别目标

对每个标的定义观测残差 $x_t=\log(I_t/A)$，再将其分解为：

$$x_t=u_t+\ell_t^\top f_t+q_t+\nu_t,$$

其中 $u_t$ 是待验证的 anchor-specific mispricing，$f_t$ 是决策时可见的全球/类别/FX 因子，$q_t$ 是指数构造、合约映射或时段性偏差，$\nu_t$ 是观测误差。策略不能假定 $x_t=u_t$。

结构目标不是声称“闭市随机导致回归”，而是识别条件收缩函数：

$$g_h(u,z)=E[u_{t+h}-u_t\mid u_t=u,Z_t=z,valid\ anchor,closed],$$

并检验可交易区域内 $u\,g_h(u,z)<0$、长期中心等价于 0、且效应强于负对照。若顺序可忽略性、平行趋势或有效工具变量等识别假设不成立，只能报告稳健条件关联，禁止使用 causal 字样。

$f_t$ 至少覆盖 TradFi 横截面共同成分、BTC/加密风险、相关美股/指数期货、USD/CNY 或 USD/HKD reference、波动与流动性；任何因子都必须满足当时可得和 source-time 审计。

## 70. 正交化检验与机械均值回归防护

对 $Y_h=x_{t+h}-x_t$、$X=x_t$ 和预处理控制量 $Z_t$，在 session-block 外样本拟合 $m_h(Z)=E[Y_h|Z]$ 与 $e(Z)=E[X|Z]$，构造：

$$\tilde Y_h=Y_h-\hat m_h(Z),\quad \tilde X=X-\hat e(Z),$$

$$\hat\theta_h=\frac{\sum \tilde X\tilde Y_h}{\sum \tilde X^2}.$$

该 Neyman-orthogonal score 降低共同因子拟合误差对收缩系数的一级敏感性。主实现用可解释 ridge/GAM/稀疏线性模型；时间序列 DML 只作离线 challenger，并采用带 purge/embargo 的 forward block cross-fitting，不能随机打散 tick。

由于 $Y_h$ 含 $-x_t$，同一时点的测量噪声会机械制造负系数。必须同时执行：

- 使用 Binance index 作为主结构价格，mid/mark 只作平行稳健性检验；
- 用 pre-window 多快照稳健均值定义 $X$，future window 独立定义结果；
- 以足够滞后的残差或独立参考源作 IV/errors-in-variables sensitivity；
- 排除最短 horizon 后复核，报告噪声方差从 0 到上界的 SIMEX/状态空间敏感度；
- 对 stale/step/index-component update 单独分层，不把一次指数刷新当经济回归。

若去除机械噪声后效应消失，H-drift 判为 REJECTED，而不是调大交易阈值。

## 71. 闭市特异性与反事实证据

在官方收盘和下一次开盘两侧建立预注册 event-time panel，估计：

$$\Delta_h x_t=\alpha_{i,s,h}+\beta_h x_t+\delta_h(x_t\times Closed_t)+\Gamma_h Z_t+\epsilon_{t,h}.$$

$\delta_h<0$ 表示闭市相对可比开市窗口的额外收缩，但只有在无提前反应、可比窗口和平行趋势诊断通过时才具有准因果解释。否则保留为机制特异性证据。

增加三重对照：同一标的开市窗口、同一时刻但非 A/HK 锚点的 TradFi 合约、同一残差幅度的日期置换锚点。官方固定锚点必须在收缩速度、零中心等价和 deadline first-passage 三项均胜出。

实施 partial-identification sensitivity：给未观测共同冲击与 $X,Y$ 的相关强度设置区间，输出使 $\theta_h$ 翻号所需的最小 confounding strength。结论必须显示 point estimate、orthogonal estimate、IV/EIV 区间和最坏敏感度边界。

这层验证仍独立于策略行为；M1–Mxx 的下单、成交和持仓不能进入结构样本筛选。

## 72. S2 有限样本可信集合，而非单点校准

分离 aleatoric path risk、parameter uncertainty、model-form discrepancy 和 observation censoring。对 simulator family $S_k$ 定义下一 session 预测统计向量 $T$，并用 calibration-only 数据形成：

$$C_{1-\delta}=\{(k,\theta):d_w(T^{real},T^{sim}_{k,\theta})\le r_{1-\delta}(z)\}.$$

$d_w$ 同时包含路径、条件响应和尾部距离；$r_{1-\delta}(z)$ 由按 session 的 block bootstrap 与 prequential conformal residual 校准，不允许根据 M7 收益调节。

动态可信分布集合：

$$\mathcal U_t=\bigcup_{(k,\theta)\in C_{1-\delta}}\{Q:AW_c(Q,P_{k,\theta})\le\rho_t(k,\theta,z_t)\},$$

其中 adapted Wasserstein 保留信息流；模型索引、参数和 discrepancy 必须联合变化。分别挑选最温和的市场、执行和系统模型后再相乘属于非法乐观组合。

Conformal 层只校准 loss/quantile 的经验覆盖，不宣称在任意金融非平稳序列中分布无关。报告 rolling coverage、stress-cell coverage、coverage gap、interval width 和 effective sample size；ESS 过低或近期连续超界时扩大半径直至 HALT。

2025–2026 的 anytime/conformal 方法仅作 challenger；通过合成真值、历史锁箱和 fault injection 后才能成为安全边界来源。

## 73. 极端市场与破产概率的稀有事件求解

普通 Monte Carlo 中“跑了 N 条路径、一次没爆仓”不能证明安全。若独立路径零失败，单侧置信度 $1-\alpha$ 下的失败概率上界仍为：

$$p_{fail}^{upper}=1-\alpha^{1/N}\approx-\log(\alpha)/N,$$

即 95% 单侧置信上界约为 $3/N$。所有报告必须显示该上界，禁止输出 ruin probability=0。

S2 stress ensemble 对共同跳跃、流动性抽空、mark-index 脱钩、取消失败和延迟风暴使用 multivariate POT/regular-variation tail；bulk 与 tail 在冻结阈值处连续拼接，尾部依赖用极端相关或 tail copula 校准。

对 $\tau_{ruin}$、强平、无法在开盘前退出等罕见事件使用 adaptive multilevel splitting：按 margin slack、drawdown 和 unresolved-order exposure 设置中间 level，复制接近危险边界的路径，使每层存活样本量近似稳定。

再用独立 importance-sampling/命名 stress seeds 交叉验证。每项输出估计值、置信区间、有效样本量、level sensitivity、最可能失败路径和 simulator family 贡献；两种估计不一致时取更坏上界并标记未认证。

## 74. M7 时间一致的词典序鲁棒控制

扩展状态为 $z_t=(belief,orders,inventory,cash,margin,H_t,W_t,time\ to\ open,health)$，其中 $H_t=\max_{u\le t}W_u$，drawdown state 为 $D_t=1-W_t/H_t$。

第一层 Safety Shield 求鲁棒可生存动作集：

$$\mathcal A_{safe}(z_t)=\{a:\sup_{Q\in\mathcal U_t}P_Q(\tau_{liq}\wedge\tau_{ruin}\le T|z_t,a)\le\epsilon_{ruin},\ h_j^{worst}\ge0\ \forall j\}.$$

第二层在安全域内最大化动态鲁棒增长：

$$V_t(z)=\max_{a\in\mathcal A_{safe}}\inf_{Q\in\mathcal U_t}\left\{E_Q[\Delta\log W_{t+1}-c_t]+\mathfrak R_t^Q[V_{t+1}]\right\},$$

$\mathfrak R_t$ 使用嵌套条件风险映射，而不是每天重算一个静态 CVaR；否则昨天认为可接受的计划可能在今天产生时间不一致反转。

第三层对近似同值动作依次最小化 drawdown upper bound、尾部模型敏感度、换手、未决订单复杂度；鲁棒价值相对 NO_ACTION 未超过统计误差与 solver gap 时必须 abstain。

Risk-constrained Kelly 只提供安全规模上界，不直接决定仓位。最终规模还需同时满足保证金、可退出深度、队列/延迟、组合共同跳跃和开盘 deadline 约束。

## 75. 一万元动态资金与“好市场强收益”机制

总资本仍为 10,000 CNY 报告口径；交易账本以实际 USDT 余额为真值，CNY 仅按有时间戳的 FX 转换展示，不混入自融资约束。

每个候选 cell 的风险乘子定义为：

$$r_{cell}=r_{evidence}\,r_{regime}\,r_{liquidity}\,r_{deadline}\,r_{drawdown},\quad 0\le r_{cell}\le1,$$

其中 evidence 来自 Round 6 独立假设层，其他项来自实时可观测风险。任一 REJECTED/Broken/stale 项使乘子为 0。

组合规模：

$$q^*=\min(q_{viability},q_{margin},q_{exit},q_{drawdown},q_{robustKelly})\times r_{cell}.$$

“大好市场强收益”不靠放宽硬约束，而来自：更多 SUPPORTED cell、更窄模型集合、更高 target-before-adverse 概率、更低成本和更大的可退出安全容量。证据增强可平滑扩容；单次盈利、短窗 Sharpe 或 posterior 均值跳升不能突然加杠杆。

引入 capital recovery hysteresis：回撤后降额快、恢复慢；只有累计 evidence CS、风险覆盖和账户对账连续达标才逐级恢复，防止震荡状态频繁放大/缩小仓位。

## 76. 可计算求解器与最优性证书

离线层：对 symbol × regime × time-to-open 网格求 jump-OU first-passage PIDE、viability kernel 和 terminal value；使用 monotone finite-volume/semi-Lagrangian scheme，并以网格加密、边界扩张和 Monte Carlo 首达交叉验证误差。

在线层采用两阶段：先枚举有限首动作并由 Safety Shield 剪枝；再对 7 标的规模做 convex/conic master allocation。最坏分布用 causal-DRO dual/cutting-plane oracle，发现新 adversarial path 后加入约束直至上下界收敛。

每次决策输出：

- primal feasible value 与 dual/worst-case upper bound；
- absolute/relative optimality gap 和 discretization/model-error bound；
- binding risk constraints、shadow prices 和最坏 adversarial scenario；
- solve time、iteration、cache/grid version 与 deterministic replay hash；
- timeout/failure 时采用的已验证 incumbent。

硬时限内无可行证书时，不返回未经验证的近似最优动作：有安全持仓则保持/减仓，无安全持仓则 NO_ACTION，必要时 FLATTEN/HALT。任何所谓“超强数学最优”必须落到可复现 gap，而不是模型复杂度。

数值测试包含 manufactured solution、PIDE 与 path simulation 一致性、对偶上下界夹逼、单调性、极限情形和浮点舍入下的硬约束保守性。

## 77. 在线失效监控、日志与新指标

为每个 evidence cell 维护 effect/center/coverage 三类相关变化监控：不是检测任意微小漂移，而是检测是否越过经济相关 corridor。触发后先 Caution/降额，再由确认窗口决定 Broken；恢复同样需要等价性证据。

新增事件：IdentificationSnapshot、FactorSnapshot、MeasurementErrorAudit、SimulatorSetSnapshot、CoverageException、RareEventCertificate、RiskBudgetSnapshot、DROAdversary、SolverCertificate。全部关联 evidence_id、decision_id、model/data/config hash。

模拟器新增指标：prequential log/energy score、PIT 与 coverage、conditional tail coverage、Wasserstein/MMD、extreme co-jump、ruin upper bound、AMS variance、reality-gap decomposition、策略排名跨 simulator family 的 Kendall/Spearman 稳定性。

策略新增指标：robust log-growth、nested-risk cost、ruin/liq/drawdown 上界、safe-capacity utilization、evidence-to-size 单调性、recovery hysteresis、NO_ACTION option value、adversarial regret、solver gap-adjusted value。

核心机制新增指标：raw/orthogonal/IV-EIV 收缩系数、closure incremental effect、confounding flip threshold、measurement-noise sensitivity、官方锚点 placebo dominance 和 effect-corridor breach duration。

全部指标仍按 method × symbol × direction × residual bucket × session phase × regime 输出，并明确区分事实、估计、置信边界、模型最坏值和命名压力值。

## 78. Round 7 门禁与实施顺序

R7-G0 Identification：数据谱系、因子可得性、测量误差防护、负对照和识别假设审计通过。
R7-G1 Simulator Set：S2 每个可信 family 在 lockbox 的路径、条件、尾部与 coverage 达标；不达标模型不得支持策略升级。
R7-G2 Tail Certificate：AMS 与独立估计一致，ruin/liq 上置信界低于预注册容忍度；零失败不按零风险。
R7-G3 Solver：所有硬约束、对偶 gap、离散误差、timeout fallback 和 deterministic replay 通过。
R7-G4 Shadow：只读真实流上 coverage、识别状态、延迟和求解时限连续达标。
R7-G5 Full Matrix：M1–M7 × 7 标的 × factual/strict/stress 全并行；任何版本不得获得更优数据或模拟器。

实施依赖保持：先完成 Round 5 P0–P3；随后 Round 6 H0–H2 与 Round 7 I0（识别样本/因子审计）并行；再完成 S2 credible set、tail engine 和 solver certificate；最后才允许重启完整实验。

Round 7 仍是设计轮：不修改 engine、不编译、不启动实验。下一代码工作不应直接写 M7 仓位公式，而应先实现统一事件、锚点谱系、ValidationClaimOpportunity 与 simulator calibration split。

## 79. Round 7 研究依据与采用边界

- [Causal DRO duality](https://arxiv.org/abs/2401.16556)：支持 adapted Wasserstein、动态对偶和非前视最坏分布。
- [Risk-averse control by nested risk measures](https://pubsonline.informs.org/doi/10.1287/moor.2022.1314)：支持时间一致风险递归与动态规划。
- [Risk-Constrained Kelly](https://stanford.edu/~boyd/simulations/kelly.html)：支持增长目标下显式 drawdown probability bound；仅作规模上界。
- [Distributionally Robust Kelly](https://web.stanford.edu/~boyd/simulations/robust_kelly.html)：支持概率分布不确定下最坏 log-growth 和可解凸形式。
- [Multilevel Splitting](https://pubsonline.informs.org/doi/10.1287/opre.47.4.585)：支持高效估计普通 Monte Carlo 难以覆盖的罕见失败概率。
- [Monitoring relevant changes](https://arxiv.org/abs/2509.01756)：支持围绕经济相关 corridor 的长期变化监控，而非检测任意微小变化。
- [Anytime-valid conformal risk control](https://arxiv.org/abs/2602.04364) 与 [regime-weighted VaR calibration](https://arxiv.org/abs/2602.03903)：只作 2026 challenger，必须验证非平稳依赖下 coverage。
- [DML for time series](https://arxiv.org/abs/2603.10999)：只作正交化 challenger；其时间可逆/依赖假设不满足时不得用于正式因果结论。

## 80. Round 8 核心结论：先纠正交易所契约，再谈最优策略

截至 2026-09-04，Binance 对股票类 TradFi Perps 的闭市定价机制已经发生决定性变化：自 2026-05-16 00:00 UTC 起，股票类合约在日常维护、闭市、周末和节假日进入 Orderbook EWMA 模式；Index Price 不再是静态官方收盘价，而是由本合约订单簿的 Impact Bid/Ask 推导 Impact Mid，再经 EWMA、移动限制和平滑切换得到。港股 USDT-Priced 合约自 2026-07-22 03:00 UTC 起还引入 USDT/HKD 转换；Quanto 合约则按本地货币报价、以 USDT 结算，不能与 USDT-Priced 合约共用同一换算公式。

这带来一个 P0 级结论：闭市期间 Binance Index、Mark、Mid、Last 与订单簿不是四个独立价格源。Index 由订单簿派生，Mark 又受 Index 与合约价格共同约束；用闭市 Index 验证合约相对官方收盘的“结构回归”，会形成内生循环。Round 7 中“Binance index 作为主结构价格”的表述仅适用于外部供应商主导的 regular mode；在 Orderbook EWMA mode 必须撤销。

官方收盘锚点仍固定，但它的角色改为历史边界条件，不再被解释为闭市期间真实公平价。AnchorBell 的可交易问题应改写为：

1. 官方收盘之后，未上市交易的股票存在一个不可直接观测的 next-open latent fair value；
2. 合约订单簿对新信息进行 24/7 价格发现，既可能暂时偏离，也可能正确重估；
3. 可交易 edge 不是“合约必然回到旧收盘”，而是“合约价格相对潜在公平价和未来开盘分布的偏差，扣除成交、资金费、尾部和退出成本后仍为正”；
4. 若潜在公平价不可识别或置信区间覆盖当前可交易价格，正确动作是 NO_ACTION。

Round 8 不创建 M8。它修正 S2、Round 6 假设层和 M7 的输入语义，并建立可落地的下一轮数学与实现契约。

## 81. 版本化交易所定价契约

每个 symbol × timestamp 必须先解析 PriceMode，而不是仅凭“股票开盘/闭盘”二分：

\[
Mode_t\in\{RegularVendor,FastDecayEWMA,SlowDecayEWMA,Fixed,OrderbookEWMA,Transition,Unknown\}.
\]

PriceModeSnapshot 至少记录：

- 合约类型：Chinese Equity、Hong Kong USDT-Priced、Hong Kong Quanto、US Equity、Pre-IPO；
- 生效规则版本、来源 URL、公告时间和本地抓取时间；
- regular/extended/overnight/maintenance/holiday 状态；
- Index/Mark 模式、转换币种、转换率及时间戳；
- transition start、winLen（若交易所未公开则为 Unknown）和模式切换原因；
- Mark/Index deviation cap、价格移动限制及其是否由公开规则精确给出；
- corporate action、dividend adjustment、合约迁移和临时公告。

规则未知或版本缺口不是普通数据缺失，而是 ContractUnknown：禁止增加风险。不得把 2026-05-16 之前 Fixed-mode 数据与之后 Orderbook-EWMA 数据直接拼接估计同一过程。所有假设检验、模拟校准和策略指标必须按规则版本分层。

对港股定义两类不可混淆锚点：

\[
A_t^{USDT}=S_{close}^{HKD}\times FX_{HKD/USD,t}\times FX_{USD/USDT,t},
\]

\[
A_t^{Quanto}=S_{close}^{HKD},
\]

第二式仅表示合约报价单位的一一映射，不表示经济上 HKD 与 USDT 等值。账本 PnL、保证金和组合风险仍必须在真实 USDT 价值下统一。

## 82. 潜在公平价与双残差状态空间模型

令 \(a=\log A_{close}\) 为固定官方收盘锚，\(f_t\) 为不可观测的 next-open latent log fair value，\(p_t\) 为合约可交易价格。分解：

\[
p_t-a=\underbrace{(f_t-a)}_{n_t:\ information\ revaluation}
+\underbrace{(p_t-f_t)}_{m_t:\ microstructure\ mispricing}.
\]

旧策略直接交易总残差 \(x_t=p_t-a\)，隐含假设 \(n_t=0\)。Round 8 必须分别推断 \(n_t\) 与 \(m_t\)。只有 \(m_t\) 的后验方向、可实现幅度和及时收敛概率足够明确时，才存在均值回归交易依据。

潜在公平价采用带跳跃、异方差和制度切换的连续时间模型：

\[
df_t=\beta_{r_t}^{\top}dF_t+b_{r_t}(t)dt+\sigma_{r_t,t}dW_t+dJ_t,
\]

其中 \(F_t\) 只含决策时已知的全球股票、行业、相关 ADR/OTC/指数期货、FX、利率、加密风险和经审计新闻代理；\(r_t\) 是隐含信息状态；\(J_t\) 表示财报、政策、公司事件与宏观新闻跳跃。对无可靠外部代理的标的，跳跃不确定性必须扩大后验区间，而不是被 OU 回归项吸收。

观测模型显式区分内生性：

\[
y_t^{ext}=H_t^{ext}f_t+\epsilon_t^{ext},
\]

\[
y_t^{book}=f_t+m_t+\epsilon_t^{book},
\]

\[
Index_t^{closed}=\mathcal E_{\theta_t}(Book_{0:t})+\epsilon_t^{idx},
\]

\[
Mark_t=\mathcal M(Index_{0:t},ContractPrice_{0:t},Funding_t)+\epsilon_t^{mark}.
\]

闭市 Index 与 Mark 只能作为订单簿状态和清算风险观测，不能作为 \(f_t\) 的独立外部 measurement。过滤器使用 Rao-Blackwellized particle filter 或 switching state-space filter；线性高斯子块解析更新，跳跃/制度状态用粒子更新。输出完整 belief，不只输出点预测：

\[
b_t=P(f_t,m_t,r_t,\theta_t\mid\mathcal I_t).
\]

必须报告 posterior width、jump probability、model disagreement、external-source coverage 与 mode-specific calibration。后验宽度超过可交易 edge 时直接 abstain。

## 83. 从“回到锚点”改为可证伪的开盘结算目标

核心研究终点改为下一次官方市场可交易价格，而不是闭市内生 Index。定义：

\[
Y_{open}=\log S_{\tau_{open}+\Delta}^{official},
\]

其中 \(\Delta\) 使用预注册的开盘稳健窗口，规避单笔集合竞价异常。估计目标为：

\[
\pi_t=P(Y_{open}-p_t\text{ 与交易方向同号且净幅度}>c_t\mid\mathcal I_t),
\]

以及竞争风险：

\[
P(\tau_{target}<\tau_{adverse}\wedge\tau_{deadline}\mid\mathcal I_t).
\]

必须并行验证三个命题：

- Anchor information retention：旧收盘对 next-open 是否仍有增量预测力；
- Information displacement：外部夜间信息是否已经使旧收盘失效；
- Microstructure correction：在控制潜在公平价后，剩余 \(m_t\) 是否有可及时实现的收敛。

“总残差收缩”不再足以支持策略。只有第三项的保守净边际下界为正，且第一、第二项没有显示结构性重估占主导，cell 才可 SUPPORTED。

使用层级贝叶斯模型在标的间共享统计强度，但保留 symbol/regime 随机效应和重尾残差。层级收缩只能改善估计，不能让证据不足标的借用强标的结论自动通过门禁。

## 84. 内生订单流、队列和反事实执行世界

S2 分成三个不可混用的世界：

1. Factual Replay：重放真实外生事件，只适用于自身订单足够小、不会改变市场路径的估值；
2. Structural Queue World：用价时优先、订单生命周期、延迟和标记点过程模拟可解释的局部反事实；
3. Generative Stress World：生成未发生的流动性、跳跃与故障路径，只用于稳健性和尾部认证。

结构队列世界的事件强度写为多维 marked point process：

\[
\lambda_k(t)=\phi_k(z_t)+\sum_j\int_0^t g_{kj}(t-s,mark_s)dN_j(s),
\]

事件类型覆盖各档新增、撤单、主动买卖、价格跳变、深度恢复和断线。自身订单 \(a_t\) 必须进入状态转移：

\[
P(z_{t+1}\mid z_t,a_t),
\]

否则无法回答“如果我挂了这笔单，后续队列和订单流是否改变”。对小额 Maker 单，允许以 influence bound 证明近似无影响；超过阈值后必须进入 impact-aware 世界，不能继续使用纯历史 replay。

FlowLOB 等生成模型可作为 Generative Stress challenger：其优点是可按趋势、波动、流动性和 imbalance 条件生成、采样成本较低，并能测试反事实条件是否真的移动目标统计量；但它不提供交易所语义正确性，也不能替代价时优先、ACK/cancel race、账户和清算状态机。任何深度生成模型必须包在交易所状态机外侧，并通过 held-out symbol、tail cell、conditional response 和策略不变量审计。

## 85. 成交概率与成交后价值的联合模型

每个候选 Maker 动作 \(a=(side,price,size,cancel\ policy)\) 的价值不能拆成独立 fill probability 与 alpha：

\[
V(a)=E[\mathbf1_{fill(a)}(Y-p_{fill})-Fee-Funding-ExitCost-Impact\mid\mathcal I_t,a].
\]

成交本身是信息事件，需联合估计：

\[
P(fill,\ MO_{1s},MO_{5s},MO_{30s},Y_{open},cancel\ race\mid b_t,a).
\]

采用 competing intensity/marked survival 模型估计 fill、partial fill、adverse move、cancel success 和 deadline。校准按 price distance、queue interval、imbalance、trade intensity、latency state、symbol 和 regime 分层。策略优化使用联合分布的下置信边界，禁止用“高成交率 × 无条件预期收益”相乘。

对撤单/改单加入 queue-reset option value。频繁重报价的损失包括：失去队列优先级、未决状态风险、消息限频和选择性成交。最优 quote persistence 应由 Bellman/MPC 比较决定，而不是固定 750ms 常数。

## 86. 部分识别 OPE 与不可评估区域

历史数据只能覆盖行为策略实际访问的状态—动作区域。POMDP 中历史依赖策略的 model-free OPE 可能具有指数级样本复杂度；不存在 coverage 时，复杂估计器不能创造信息。

为每个目标策略输出 OPE support certificate：

- action/history coverage ratio；
- effective sample size 与最大 importance weight；
- belief/outcome revealing diagnostics；
- behavior policy 与日志版本；
- 未观测混杂敏感度；
- identified value interval，而非强制点估计。

若 target action 超出历史 support，允许的结论只有：

1. 进入结构模拟器并扩大模型不确定性；
2. 进行预算受限的安全探针收集数据；
3. 标记 Unidentified 并拒绝上线。

禁止在无 support 区域用同一模拟器生成数据、选择策略、再用该模拟器证明策略优秀。模型选择、策略优化和最终评估必须使用 discovery/calibration/lockbox 三重隔离，并保留真实 shadow forward evaluation。

## 87. 安全价值信息探针

小额 Maker 探针不是为了赚取即时收益，而是购买队列、延迟、成交选择性和模型区分信息。定义动作的信息价值：

\[
VOI(a)=E[\mathcal L(b_t)-\mathcal L(b_{t+1})\mid a]-C_{exec}(a)-C_{risk}(a),
\]

其中 \(\mathcal L\) 可取决策后悔上界或后验熵，但只有在硬风险可行域内且最坏损失受限时才允许探针。

探针调度解预算背包/实验设计问题：

\[
\max_{\{a_i\}}\sum_i VOI(a_i),\quad
\sum_i RiskUpper(a_i)\le B_{probe},\\
Exposure^{worst}\le L.
\]

同一 cell 达到校准精度后停止探针；高风险、低流动性、临近开盘或规则未知状态禁止探索。探针 ledger 与收益 ledger 分离，避免把研究成本伪装成策略亏损或把偶然利润当作 alpha。

## 88. 分层词典序控制与可实时求解

完整问题是 belief-state、部分识别、分布鲁棒、带订单状态的随机控制，直接在线求全局 POMDP 最优不可行。采用可证书化分解：

### 88.1 Layer 0：不可绕过的确定性契约

验证 PriceMode、数据新鲜度、账户对账、交易规则、保证金、订单唯一性和开盘/资金费 deadline。失败即 HALT/REDUCE_ONLY。

### 88.2 Layer 1：鲁棒 viability shield

对 belief set 与 simulator credible set 求有限时域可生存集合，剪除在任一可信情景下可能违反硬约束的动作。使用 backward reachable set、barrier certificate 与 interval arithmetic 保守计算。

### 88.3 Layer 2：有限候选报价 MPC

每标的生成有限候选：NO_ACTION、保持、若干 Maker 价格/规模、取消、Maker 减仓、紧急退出。对每个候选进行短时域 scenario-tree rollout，目标包含净 edge、联合 fill-markout、库存、deadline 与 terminal liquidation。使用 progressive hedging 或 scenario reduction 控制延迟。

### 88.4 Layer 3：组合锥优化

把每个候选动作的保守收益、保证金、因子暴露、共同跳跃、退出容量和最坏损失传给组合 master：

\[
\max_q\ \underline\mu^\top q-\lambda_{turn}\|q-q_0\|_1
\]

subject to

\[
q\in\mathcal Q_{discrete},\quad
Aq\le b,\quad
\sup_{Q\in\mathcal U}CVaR_\alpha^Q(-R(q))\le C,\quad
P_Q(D_T>d_{max})\le\epsilon.
\]

小规模离散候选使用 MI-SOCP；热路径可先固定每标的候选再解 SOCP。Benders/cutting-plane 由最坏情景 oracle 增量加入约束。任何 relaxed solution 必须 rounding 后重新通过 viability 与保证金验证。

### 88.5 Layer 4：词典序择优与拒绝权

先满足生存与规则，再最大化 gap-adjusted worst-case net value；近似同值时依次最小化回撤、模型敏感度、换手、未决订单和求解复杂度。若：

\[
LCB(V(a)-V(NO\_ACTION))\le
solverGap+modelError+executionError,
\]

则选择 NO_ACTION。复杂模型没有拒绝权就不是安全优化器。

## 89. 最优性、真实性与经济价值三类证书

每次决策证书必须拆成三类，禁止用单一“置信度”混合：

- SolverCertificate：原始可行值、对偶上界、MIP/锥 gap、迭代、超时 incumbent；
- ModelCertificate：belief coverage、credible-set 半径、离散误差、simulator family disagreement；
- EconomicCertificate：相对 NO_ACTION 的净价值下界、成本瀑布、first-passage 概率、最坏退出成本。

总保守优势定义为：

\[
Adv_{cert}=\underline V_{action}-\overline V_{noaction}
-\epsilon_{solver}-\epsilon_{disc}-\epsilon_{sim}.
\]

只有 \(Adv_{cert}>0\) 且所有硬约束证书通过才能增险。报告“理论最优”必须同时限定信息集、模型集合、动作集、时域、求解容差和规则版本。

## 90. 参数校准与防止数学模型过拟合

参数不按 PnL 调优。采用以下独立损失：

- 潜在公平价：next-open proper scoring rule、coverage 与 calibration slope；
- 队列/成交：联合 survival likelihood、Brier score、conditional fill/markout coverage；
- 模拟器：路径统计、条件响应、极端共跳、故障恢复和策略排名稳定性；
- 控制器：lockbox regret、约束违例上界、证书覆盖和 NO_ACTION 选择质量。

所有模型使用滚动 prequential 更新；超参数只在过去窗口选择，当前 session 仅预测。模型集合权重通过 stacking/minimax regret 更新，但设置最小幸存权重，防止短期赢家完全挤出尾部模型。结构断点触发重新校准，不允许跨 PriceMode 版本静默迁移参数。

阈值由经济量反推：

\[
edge_{min}=fee+funding+queueCost+adverseSelection+
exitCost^{upper}+modelError+solverError+safetyBuffer.
\]

入场阈值不是固定 bps，也不是某个分位数；它是 symbol × state × action 的动态净成本和误差上界。

## 91. Round 8 实施切片与依赖顺序

R8-P0：实现版本化 PriceModeSnapshot，修正 2026-05-16 后闭市 Index 的内生语义；港股 USDT-Priced/Quanto 和 FX 路径严格分开。

R8-P1：把 Anchor residual 拆为 information revaluation 与 microstructure mispricing；实现 AnchorLineage、ExternalFactorSnapshot、LatentFairValueBelief 和开盘 outcome worker。

R8-P2：重写 Round 6 opportunity schema，使 primary endpoint 为 official next-open；闭市 Index/Mark 仅作为内生订单簿与清算观测。

R8-P3：实现 OPE support certificate；无 coverage 的策略只允许模拟器压力研究或安全探针，不得进入候选上线集。

R8-P4：S2 拆成 factual replay、structural queue、generative stress 三种明确 world type；实现 influence bound 和 counterfactual action channel。

R8-P5：实现联合 fill-markout-deadline 模型、quote persistence 与 cancel race；取消固定成交概率乘无条件 alpha 的估值方式。

R8-P6：先实现有限候选 MPC + 组合 SOCP，再接入 causal-DRO oracle；每一步都输出 solver/model/economic 三证书。

R8-P7：实现 VOI probe ledger 与独立风险预算；只在 shadow/极小规模、完整停止规则下校准。

R8-P8：完整矩阵变为 M1–M7 × 7 标的 × PriceMode × factual/structural/stress；旧版本保留，但任何依赖闭市 Index 独立性的指标标为 semantically invalid，不能参与排名。

严格依赖顺序：P0 → P1/P2 → P3/P4 → P5 → P6 → P7 → P8。P0 未完成前禁止启动新的收益比较，因为数据语义已经改变。

## 92. Round 8 门禁与研究依据

R8-G0 Contract：逐 timestamp 的 PriceMode/FX/合约类型可重放，未知状态 fail closed。

R8-G1 Identification：next-open endpoint、外部因子 source-time、Index 内生性和机械回归审计通过。

R8-G2 Counterfactual：三类 world 不混用；结构队列能响应自身动作；生成模型通过 conditional validity，而不只通过边际分布相似度。

R8-G3 OPE：覆盖、ESS、混杂敏感度和 identified interval 完整；Unidentified 策略不得被宣称优于基线。

R8-G4 Optimization：候选动作全部经 viability shield；MI-SOCP/SOCP 与 adversarial oracle 给出可复现 gap；超时回退到已验证 incumbent。

R8-G5 Economics：扣除全部成本与三类误差后，Adv_cert 下界为正；否则 NO_ACTION。

R8-G6 Forward：在未见 shadow session 上连续通过 PriceMode 分层校准、风险覆盖和运行时预算，才允许极小资本 canary。

研究与规则依据：

- [Binance TradFi Perps pricing](https://www.binance.com/en/support/faq/detail/fe7dcdf24f1943d98b368f5f9f744398)：Orderbook EWMA、模式切换、Mark/Index 约束、FX 与各市场时段的当前官方契约。
- [Binance equity Orderbook EWMA notice](https://www.binance.com/en/support/announcement/detail/53bfc17634f54f2f90666dbc396f5cee)：股票类 TradFi Perps 自 2026-05-16 起切换闭市 Index 计算模式。
- [FlowLOB](https://arxiv.org/abs/2608.13096)：支持高效、条件可控、跨标的的生成式 LOB challenger；仅进入 stress world。
- [Signature-Based Optimal Execution](https://arxiv.org/abs/2606.31387)：说明路径依赖 alpha 与执行可约化为有限维凹二次问题；可作为离线候选特征 challenger。
- [Model Predictive Control for Trade Execution](https://arxiv.org/abs/2603.28898)：支持以快速 QP 平衡完成度、冲击、机会成本和风险的生产化分层求解。
- [History-dependent OPE in POMDPs](https://arxiv.org/abs/2503.01134)：说明历史依赖 POMDP 的 model-free OPE 在缺少 coverage/revealing 条件时可能不可处理，并支持 model-based 分支。
- [Robust finite-memory policies for hidden-model POMDPs](https://arxiv.org/abs/2505.09518)：支持跨隐藏环境集合的最坏模型验证与有限记忆策略优化。

Round 8 至此完成数学与实施契约打磨，尚未修改 engine 代码、未编译、未启动新实验。下一步必须从 R8-P0 开始，而不是直接调阈值、仓位或收益。

## 93. Round 9 核心结论：Orderbook-EWMA 使市场、策略与风险参考价形成闭环

Round 8 识别出闭市 Index 的内生性，但仍把“市场订单簿”近似为不含自身作用的观测对象。真实系统中，若我方挂单进入 Impact Bid/Ask 的累计深度，动作会通过以下路径产生反馈：

\[
a_t\rightarrow Book_t^{all}\rightarrow ImpactMid_t
\rightarrow Index_t^{EWMA}\rightarrow Mark_t
\rightarrow Margin_t,\ Signal_t,\ FundingExpectation_t.
\]

因此动作不仅影响成交概率和队列，还可能影响参考价、表观残差、未实现盈亏、清算距离以及下一轮决策。即使资金规模很小，也不能在数学上默认该导数为零；必须先计算影响上界。

Round 9 的首要原则是：任何策略价值、alpha 和风险计算都必须同时维护 observed world 与 self-excluded counterfactual world。策略绝不以改变 Index/Mark 为目标，不把自身引起的账面改善计入经济收益，也不允许由自反馈形成重复加仓环。

## 94. 自身剔除的反事实订单簿与参考价格

记公开聚合订单簿为 \(B_t^{all}\)，我方所有已确认和可能仍存活的 Unknown/PendingCancel 订单集合为 \(O_t^{self,+}\)。由于未知状态不能安全地假设已撤销，定义订单存在区间：

\[
O_t^{self}\in[\underline O_t^{self},\overline O_t^{self}],
\]

其中下界只含确定存活订单，上界还含所有未完成对账的可能存活订单。构造 self-excluded book interval：

\[
B_t^{-self}\in
[B_t^{all}\ominus \overline O_t^{self},
 B_t^{all}\ominus \underline O_t^{self}].
\]

在交易所公开算法与参数可复现时，对每个簿计算：

\[
I_t^{obs}=\mathcal E_\theta(B_{0:t}^{all}),\qquad
I_t^{-self}=\mathcal E_\theta(B_{0:t}^{-self}),
\]

\[
\Delta I_t^{self}=I_t^{obs}-I_t^{-self}.
\]

若 EWMA 衰减、Impact Notional、移动限制或 transition 参数未公开，则不伪造点值，而以公开约束和深度区间计算 \(\Delta I_t^{self}\) 的可达上下界。

新增 ReferenceFeedbackSnapshot：

- observed/self-excluded Impact Bid、Impact Ask、Impact Mid；
- 我方订单在 Impact Notional 扫描路径中的贡献；
- \(\partial I/\partial q_i\)、\(\partial Mark/\partial q_i\) 的区间或有限差分上界；
- EWMA memory state、规则版本、参数已知性；
- observed 与 self-excluded residual、PnL、margin slack；
- Unknown 订单导致的最坏反馈范围。

所有信号默认使用 self-excluded book；清算和账户风险同时使用 observed Mark 与 worst-reachable Mark。若无法重建 self-excluded interval，禁止增加风险。

## 95. 反自激、反操纵与经济 PnL

策略状态更新必须阻断自激环：

\[
Signal_t=\Psi(B_t^{-self},External_t,Anchor_t,Belief_t),
\]

而不是 \(\Psi(B_t^{all},Index_t^{obs})\)。动态资金和证据更新同样不能使用自身动作造成的 Index/Mark 变化作为正面证据。

将 PnL 分为：

\[
PnL^{cash}=RealizedCashflow-Fees-Funding,
\]

\[
PnL^{obs}=PnL^{cash}+Inventory\cdot Mark^{obs},
\]

\[
PnL^{-self}=PnL^{cash}+Inventory\cdot Mark^{-self},
\]

\[
PnL^{feedback}=PnL^{obs}-PnL^{-self}.
\]

策略排名使用 realized cash 与 self-excluded 保守估值；\(PnL^{feedback}\) 单独报告且不得计入 alpha。任何订单若对 Index/Mark 的最坏影响超过预注册阈值，进入 ReferenceSensitive：缩量、移价或 NO_ACTION。系统同时记录 spoof-like persistence、cancel-to-fill、order-to-trade 和 reference contribution 指标，以证明策略行为与真实成交意图一致。

## 96. 多主体价格发现与响应不确定性

闭市合约价格由异质主体共同形成：信息交易者、套利者、做市商、噪声交易者、强平流和可能的操纵者。单一 Hawkes 拟合只能描述历史平均响应，不能保证部署后其他主体仍按同一规律反应。

定义隐藏群体状态 \(\xi_t\) 与我方动作 \(a_t\)：

\[
z_{t+1}\sim P_{\theta,\xi_t}(\cdot\mid z_t,a_t,a_t^{-self}).
\]

不尝试在线求完整 Nash 均衡，而构造可校准的 response ambiguity set：

\[
\mathcal R_t=
\{R:\ d(R,R_k)\le\rho_k,\ k\in\mathcal K_t\},
\]

其中 family 至少包含：

- historical passive response；
- toxicity/amplification response；
- liquidity withdrawal；
- copy/queue-jump response；
- adversarial but rule-valid response；
- no-response boundary case。

控制器优化 \(\inf_{R\in\mathcal R_t}\) 下的价值并限制后悔。均场博弈、agent-based market 和深度生成模型只用于扩充 response family；除非在真实 shadow intervention 上校准，否则不得把其均衡当成市场真值。

新增 policy fingerprint 监控：若市场在我方特定报单后系统性撤单、追价或反向冲击，response set 自动扩张，规模收缩。这样可防止一个在静态回放中优秀、但上线后被其他参与者识别的策略持续放大风险。

## 97. 订单簿冲击的局部可识别边界

对小额挂单，采用局部干预估计而非全局冲击函数。定义订单相对深度：

\[
\chi(a)=\frac{q_a}{Q^{disp}_{same\ side}(p_a,K)}
\]

以及参考价影响量 \(\iota(a)\)、后续订单流变化 \(\delta\lambda(a)\)。只有同时满足：

\[
\overline\iota(a)\le\epsilon_I,\qquad
\overline{|\delta\lambda(a)|}\le\epsilon_\lambda,\qquad
\chi(a)\le\epsilon_Q
\]

时，Factual Replay 的无影响近似才被认证。

局部效应通过随机化极小探针、时间交错和匹配事件窗估计；探针概率、动作和停止规则预注册。对无法随机化的样本，使用部分识别边界，不把前后变化直接解释为因果冲击。任何超过局部支持的规模都进入 Structural/Stress world，模型误差随外推距离单调增加。

## 98. 开盘是随机终端机制，不是确定价格点

下一次股票开盘包含集合竞价、隔夜订单积累、涨跌停、停牌、公司行动、价格发现跳跃和数据延迟。终端状态定义为：

\[
\mathcal T=
\{OpenContinuous,OpenAuctionOnly,LimitLocked,Suspended,
CorporateAction,Delayed,Unknown\}.
\]

终端价值必须对状态条件化：

\[
G_T(q,\mathcal T,Y_T,L_T)
=q(Y_T-p_{entry})-C_{exit}(q,L_T,\mathcal T)
-\Phi(q,\mathcal T),
\]

其中 \(L_T\) 是开盘可退出流动性，\(\Phi\) 包含未能及时退出、涨跌停和持仓跨日的风险成本。不能默认“开盘前平掉”总能发生，也不能用股票开盘价直接给 Binance Maker 仓位虚构成交。

建立 opening bridge：

1. 预测官方开盘价格/状态分布；
2. 预测 Binance 在模式切换和平滑窗口内的 Index、Mark、订单簿路径；
3. 预测我方 Maker 撤退或减仓的联合完成概率；
4. 若必须在 deadline 前完成，比较 Maker、分阶段 Maker、emergency flatten 的可达成本边界。

开盘附近使用混合终端分布，而不是连续扩散；公司行动和停牌使用命名离散情景并占有非零最小概率质量。

## 99. 时间一致的动态风险预算

静态“每标的最大仓位”不足以表达临近开盘、反馈敏感度和未决订单风险。定义剩余风险资本：

\[
K_t=W_t-\operatorname{Reserve}_t,
\]

\[
Reserve_t=
MarginBuffer_t+ExitCost_t^{upper}
+PendingOrderLoss_t^{upper}
+FeedbackLoss_t^{upper}
+GapOpenLoss_t^{upper}.
\]

每个动作消耗 risk tokens：

\[
c_t(a)=
\Delta CVaR_t^{nested}
+\lambda_1\Delta Exposure_t
+\lambda_2\Delta Feedback_t
+\lambda_3\Delta DeadlineRisk_t.
\]

风险预算是具有动态可行性的资源状态，而不是每天重新归零的限额。未来所有可信路径上都需满足：

\[
K_{t+1}\ge K_{min},\qquad
\sum_{\tau=t}^{T} c_\tau(a_\tau)\le B_t^{remaining}.
\]

drawdown、对账异常、coverage breach 和模式切换使预算快速收缩；恢复必须由新的可观测证据驱动，不能仅因市场价格反弹自动扩张。

## 100. 整数动作、非前视情景树与精确求解

真实动作受 tickSize、stepSize、minNotional、最大挂单数、reduceOnly、post-only 和限频约束，连续最优解不能直接下单。对每个时点构造有限非前视情景树 \(\omega\)，决策变量包括：

\[
x_{i,k}\in\{0,1\},\quad q_i\in stepSize_i\mathbb Z,\quad
cancel_i\in\{0,1\}.
\]

同一观测历史节点上的动作必须相同，满足 non-anticipativity。优化采用词典序 mixed-integer conic program：

第一优先级最小化最坏硬约束违例 slack，并要求最优值为零；第二优先级最大化分布鲁棒净价值；第三优先级最小化换手、反馈敏感度与订单复杂度。

为满足实时预算：

- 离线生成每标的候选动作、viability cuts 和 opening terminal cuts；
- 在线用 branch-and-bound warm start、perspective cuts、Benders decomposition；
- 最坏 response/price path 由 adversarial oracle 产生；
- 先返回第一个经独立 verifier 验证的 incumbent，再继续缩小 gap；
- 超时仅允许使用已验证 incumbent，不能使用松弛解或未复核 rounding。

SolverCertificate 增加 integer feasibility、non-anticipativity residual、cut validity、floating-point safety margin 和 independent verifier hash。

## 101. 闭环分布鲁棒控制问题

Round 9 的完整有限时域问题写为：

\[
\max_{\pi\in\Pi_{finite}}
\inf_{\substack{Q\in\mathcal U_t\\R\in\mathcal R_t\\
\theta^{ref}\in\Theta_t^{ref}}}
\rho_{t:T}^{Q,R}
\left[
\sum_{\tau=t}^{T-1}
\bigl(
Cashflow_\tau-Cost_\tau
\bigr)+G_T
\right]
\]

subject to：

\[
a_\tau=\pi(h_\tau),\quad
a_\tau\in\mathcal A_{viable}(b_\tau),
\]

\[
P(\tau_{liq}\le T)\le\epsilon_{liq},\quad
P(D_T>d_{max})\le\epsilon_D,
\]

\[
|\Delta I_\tau^{self}|\le\epsilon_I,\quad
K_\tau\ge K_{min},
\]

以及全部交易所整数、状态机、deadline 和 post-only 约束。

\(\rho_{t:T}\) 使用嵌套条件风险映射保证时间一致性。模型集合 \(\mathcal U_t\)、响应集合 \(\mathcal R_t\) 和参考价参数集合 \(\Theta_t^{ref}\) 分开记录，避免把三类不确定性揉成一个不可解释半径。

若最坏模型过于保守导致长期 NO_ACTION，不能偷偷缩小集合；应使用安全探针提高可识别性，或明确承认当前数据不足以支持交易。

## 102. 在线校准与证书失效机制

Conformal/coverage 工具只负责其明确承诺的覆盖口径，不能把边际长期 coverage 等同于路径安全。维护分层 e-process/confidence sequence：

- next-open forecast error；
- fill/markout joint error；
- self-impact bound violation；
- response-family misspecification；
- terminal-state frequency；
- solver certificate failure。

每层拥有独立 alpha budget，并使用 session/symbol/regime 分组，避免重复查看导致显著性膨胀。检测到 relevant corridor breach 时：

1. 冻结受影响证书；
2. 将对应 cell 降到 INCONCLUSIVE/BROKEN；
3. 扩大 ambiguity set；
4. 取消增险订单；
5. 只有新 calibration block 恢复覆盖后才能重新启用。

不能依赖单一 adaptive conformal 方法声称任意金融非平稳过程中的条件覆盖；2025–2026 的时间序列 conformal 方法只作为候选校准器，并必须与命名 stress、coverage gap 和误差宽度联合报告。

## 103. Round 9 新日志与指标

新增事件：

- SelfOrderBookSnapshot；
- ReferenceFeedbackSnapshot；
- SelfExcludedValuation；
- ResponseSetSnapshot；
- PolicyFingerprintAlert；
- OpeningBridgeForecast；
- TerminalStateOutcome；
- DynamicRiskReserve；
- IntegerActionCertificate；
- CertificateInvalidation。

新增核心指标：

- self/reference contribution 与最坏 \(\Delta Index/\Delta Mark\)；
- observed PnL、self-excluded PnL、feedback PnL；
- influence-bound pass rate 和 extrapolation distance；
- response-family regret、policy fingerprint strength；
- opening bridge calibration、deadline completion probability；
- suspended/limit-locked/corporate-action loss；
- dynamic reserve coverage 与 risk-token utilization；
- integer gap、independent-verifier reject rate；
- NO_ACTION duration 及其事后机会成本，但不得反向用于放松安全门槛。

所有指标继续按 symbol × direction × PriceMode × regime × time-to-open 输出，并保留完整规则版本和模型 hash。

## 104. Round 9 实施切片、门禁与研究边界

实施顺序：

R9-P0：在 R8-P0 内加入 Orderbook-EWMA 参数契约、Impact Mid 重建和 self-order inventory。

R9-P1：实现 self-excluded book interval、ReferenceFeedbackSnapshot 与三套 PnL；所有策略信号切换到 self-excluded 输入。

R9-P2：实现 influence bound 与 ReferenceSensitive 状态；未通过前 simulation 模式不得把自身订单视为外生零影响。

R9-P3：实现 opening terminal taxonomy、opening bridge 和无法退出情景。

R9-P4：建立 response ambiguity set、policy fingerprint 和联合 fill-markout-response 校准。

R9-P5：实现 DynamicRiskReserve 与 risk-token 状态转移。

R9-P6：实现有限候选 non-anticipative MI-SOCP、独立 verifier 和超时 incumbent。

R9-P7：实现证书失效 e-process、alpha ledger 和恢复协议。

R9-P8：完整 shadow matrix；只在 self-feedback、opening gap、流动性撤离、Unknown orders 和 solver timeout 联合压力下通过后，才讨论 canary。

门禁：

- R9-G0：self-excluded book 在订单全生命周期和 Unknown 区间下可重放；
- R9-G1：策略收益不包含 feedback PnL，信号无自激；
- R9-G2：Factual Replay 仅在 influence certificate 通过时有效；
- R9-G3：开盘终端状态与 Binance 模式切换均被联合建模；
- R9-G4：整数动作、非前视约束与独立 verifier 全部通过；
- R9-G5：各类证书可单独失效并触发真实降险；
- R9-G6：所有可信闭环模型下 \(Adv_{cert}>0\)，否则 NO_ACTION。

研究依据与采用边界：

- [Binance TradFi Perps pricing](https://www.binance.com/en/support/faq/detail/fe7dcdf24f1943d98b368f5f9f744398)：闭市 Orderbook EWMA 以 Impact Bid/Ask 推导 Impact Mid，并影响 Index/Mark 语义；具体未公开参数必须保留区间。
- [FlowLOB](https://arxiv.org/abs/2608.13096)：支持条件可控、跨标的生成与 counterfactual distribution test；只作响应/压力 family。
- [Model Predictive Control for Trade Execution](https://arxiv.org/abs/2603.28898)：支持快速 QP、显式完成约束和 residual-value 近似；AnchorBell 扩展为整数、鲁棒和非前视版本。
- [Robust finite-memory policies for hidden-model POMDPs](https://arxiv.org/abs/2505.09518)：支持对大量隐藏环境求最坏模型与鲁棒有限记忆策略。
- [Error-quantified conformal inference](https://arxiv.org/abs/2502.00818)：支持在依赖与漂移下使用误差幅度反馈的长期覆盖 challenger；不等价于逐时条件安全保证。
- [Safe planning under environment shift](https://arxiv.org/abs/2602.12616)：支持生成先验、鲁棒 conformal 区域与 MPC 安全规划的组合；必须在金融闭环数据上独立验证。

Round 9 完成后，AnchorBell 的“最优解”不再是对一个外生价格过程调仓，而是在自身订单会影响观测、风险参考价和其他参与者响应的闭环市场中，求带整数交易规则、未知订单状态、随机开盘终端和模型集合的可验证鲁棒最优。仍不承诺未知威胁之外的绝对最优或绝对安全。

## 105. Round 10 核心目标：数学对象、软件边界与证据边界完全一致

前九轮主要补齐市场、执行、识别和控制数学。Round 10 解决另一类同样严重的问题：即使数学正确，若模拟器、策略、日志和指标在同一对象中互相调用，研究结果仍可能被实现耦合污染。

当前代码事实：

- `engine/src/simulation.rs` 约 3,790 行，同时包含 SimulationPolicyVariant、仓位分配、策略运行、订单/成交模拟、状态和报告；
- `engine/src/simulation_batch.rs` 同时承担行情接入、深度初始化、模拟器构造、多策略编排、manifest 和写盘；
- `engine/src/observability.rs` 仅有轻量 AuditKind/AuditRecord，reason 仍是自由字符串，缺少 schema/version/evidence lineage；
- `engine/src/backtest.rs` 的 FillModel 仍以 TopOfBook 为主要边界，无法表达队列区间、部分成交、延迟、取消竞态和自反馈；
- strategy 子模块已经开始拆分，但运行时仍由 SimulationEngine 聚合大量本应独立的职责。

因此下一轮实现不能继续向 SimulationEngine 添加字段。必须先建立稳定端口和依赖方向，使“换策略不改模拟器、换模拟器不改策略、改指标不改历史事实、改日志投影不改变决策”。

## 106. 六个子系统的不可变核心与可变部分

| 子系统 | 不可变核心 | 可变、可替换部分 | 严禁事项 |
|---|---|---|---|
| 市场事实层 | 原始包、source time、receive time、sequence、规则版本不可篡改 | 解析器、缓存、压缩、传输实现 | 用策略状态筛选或改写市场事实 |
| 模拟器 | 给定世界、动作和随机性后产生状态转移；不知道策略名称 | 队列模型、延迟模型、响应 family、生成模型 | 根据 M1/M7 身份给予不同成交或数据 |
| 策略 | 只从冻结 DecisionSnapshot 产生候选意图和解释 | 信号、belief、阈值、状态机、仓位逻辑 | 网络、写盘、真实时钟、直接下单、修改模拟器 |
| 求解器/安全层 | 在候选动作与约束上求可验证可行解 | MPC、SOCP、MI-SOCP、DRO oracle、近似器 | 创造 alpha、修改事实、绕过硬约束 |
| 日志/证据 | 追加式事实、全谱系、可重放、不可静默覆盖 | 编码、分片、索引、压缩、投影 | 仅记录成功路径、以指标替代原始事件 |
| 指标/研究 | 纯函数式读取冻结 ledger；定义和版本显式 | 统计量、图表、估计器、置信方法 | 指标回写历史、用测试集调策略、跨版本偷换口径 |

“不可变”指实验与语义契约不可被某个新方法改变，不代表代码永不升级。升级必须形成新 schema/model/metric version，并保留旧版可重放性。

## 107. 目标模块拓扑与单向依赖

建议将 engine 演进为以下逻辑层，而不是继续按 simulation/backtest/live 横向复制：

\[
contracts
\leftarrow market\_data
\leftarrow world
\leftarrow runtime
\]

\[
contracts
\leftarrow strategy
\leftarrow optimization
\leftarrow runtime
\]

\[
contracts\leftarrow ledger,\qquad
ledger\leftarrow metrics\leftarrow reports.
\]

具体目录契约：

- `domain/`：Price、Quantity、Money、Timestamp、Symbol、OrderState、Position 等无 I/O 值对象；
- `contracts/`：MarketEnvelope、DecisionSnapshot、ActionCandidate、ExecutionEvent、EvidenceEvent、版本化 schema；
- `market_data/`：Binance REST/WS、股票锚点、FX、日历、规则版本和 source-time；
- `world/`：factual、structural、generative 三类模拟世界及统一 WorldPort；
- `strategy/`：belief、signal、eligibility、candidate generation；保持纯函数或显式状态转移；
- `optimization/`：viability、risk reserve、MPC、portfolio master、certificate verifier；
- `execution/`：simulation/testnet/live adapter，只把已批准动作送往目标环境；
- `ledger/`：append-only journal、hash chain、checkpoint、correction；
- `metrics/`：离线投影、在线安全统计、研究检验，三者物理分开；
- `run/`：manifest、split、seed、matrix、artifact sealing；
- `application/`：CLI/dashboard，仅组合端口，不承载领域规则。

依赖检查必须自动化：strategy 不得 import world 的具体实现；world 不得 import strategy；metrics 不得被 strategy 引用；execution 不得自行产生策略意图；dashboard 不得成为状态真值。

## 108. 模拟器独立契约

定义统一端口：

\[
S_{t+1},E_{t+1}
=World.step(S_t,A_t,\Omega_t),
\]

其中 \(S_t\) 为世界状态，\(A_t\) 为标准化动作集合，\(\Omega_t\) 为显式随机性/外生事件，\(E_{t+1}\) 为世界产生的事实事件。World 看不到 method_id、PnL、Sharpe、策略阈值或实验排名。

不可变模拟器原则：

- 同一 world version、初态、动作序列、外生事件和 random tape 必须逐字节重放一致；
- M1–Mxx 使用相同外生事件和配对随机数，但各自动作导致的内生世界分叉必须独立保存；
- factual replay 只在 influence certificate 范围内使用；
- structural world 必须遵守价格—时间优先、生命周期、规则和账户守恒；
- generative world 不得伪装成历史事实；
- simulator halt、gap、Unknown、未校准区域不能被静默转为无事件。

可变组件：

- QueueKernel；
- LatencyKernel；
- CounterpartyResponseKernel；
- ReferencePriceKernel；
- MarginLiquidationKernel；
- FaultKernel；
- ExogenousPathGenerator。

每个 kernel 通过 typed interface 注入，并输出自己的 model version、calibration lineage 和 uncertainty contribution。不得把所有不确定性压成一个“realism level”。

## 109. 策略独立契约

策略状态转移定义为：

\[
(Belief_{t+1},C_t)
=\Pi_\theta(Belief_t,DecisionSnapshot_t),
\]

其中 \(C_t\) 是 ActionCandidate 集，而不是已批准订单。DecisionSnapshot 是一次冻结快照，包含：

- self-excluded 市场状态区间；
- 锚点、外部因子和潜在公平价 belief；
- 账户、持仓、未决订单区间；
- PriceMode、日历、资金费和 deadline；
- 模型/数据健康状态；
- 当前动态风险储备。

不可变策略原则：

- 不读取 wall clock；时间必须来自 snapshot；
- 不进行网络或磁盘 I/O；
- 不直接调用交易所；
- 不知道运行在 simulation、replay、testnet 还是 live；
- 相同 state + snapshot + config 必须产生相同候选与 reason codes；
- 无证据、未知规则或净优势不足时必须能返回 NO_ACTION；
- 策略只能提出动作，不能批准自己突破风险约束。

可变组件：

- LatentFairValueFilter；
- MispricingEstimator；
- EvidenceEligibility；
- CandidateQuotePolicy；
- InventoryPreference；
- DeadlinePolicy；
- ExplorationProposal。

M1–M7 应重构为 StrategyConfig/组件组合，而不是在一个大函数中按枚举逐层 if variant。每个版本的组件图写入 manifest，以便精确消融。

## 110. 求解器与安全层独立契约

策略提供候选集合 \(\mathcal C_t\)，安全层先计算：

\[
\mathcal C_t^{safe}
=\{c\in\mathcal C_t:Verifier(c,z_t,\mathcal U_t)=PASS\}.
\]

求解器仅在 \(\mathcal C_t^{safe}\) 上优化。它不估计潜在公平价、不生成信号，也不修改候选的经济含义。

输入必须包括：

- 候选动作及离散交易属性；
- 每个候选的情景现金流张量；
- 风险、保证金、反馈、退出和 deadline 约束；
- 不确定性集合；
- 求解时限与容差；
- NO_ACTION/REDUCE/FLATTEN fallback。

输出是 `ApprovedActionSet + SolverCertificate`。独立 verifier 使用另一条代码路径重新检查整数、价格方向、post-only、reduce-only、限频、风险储备和最坏情景约束。

可变求解器可以是枚举、动态规划、QP、SOCP、MI-SOCP、Benders 或近似策略；不可变的是：任何输出都必须先可行、证书完整、超时可安全回退。

## 111. 日志不是 printf，而是事实账本

建立四类物理分离、逻辑关联的追加式 ledger：

1. Source Ledger：原始 WS/REST/锚点/FX/规则响应及接收元数据；
2. World Ledger：模拟器状态转移、随机 tape、订单生命周期、故障和自反馈；
3. Decision Ledger：snapshot hash、belief、候选、拒绝原因、批准动作和三类证书；
4. Account Ledger：交易所确认、成交、费用、资金费、余额、保证金与对账。

每个事件统一 envelope：

\[
e=(schemaId,version,eventId,runId,streamId,seq,
eventTime,receiveTime,commitTime,causationId,
correlationId,payloadHash,prevHash,payload).
\]

不可变日志原则：

- append-only；错误通过 Correction/Invalidation 事件修正；
- 每 stream sequence 单调且连续；
- hash chain 与分片 Merkle root 检测删改；
- write acknowledgement 后才可声称事件持久化；
- 队列溢出必须触发明确降级，不能只增加 dropped counter；
- run 结束必须记录 StopRequested、StopCause、FinalCheckpoint、WriterFlush、ExitCode；
- secrets 在进入 ledger 前结构化剔除，而不是事后字符串替换；
- schema registry 与迁移器必须可把旧事件投影到新读模型，但不得重写原始字节。

JSONL 可继续作为可读导出格式，但不再是唯一权威存储。热路径推荐预分配二进制/WAL 分片，后台异步生成 JSONL/Parquet 投影。

## 112. 指标必须区分四种语义

指标拆成四个 namespace：

- Operational Metrics：延迟、丢包、队列、写盘、重连、求解耗时；
- Simulator Fidelity：成交、markout、路径、条件响应、尾部和现实差距；
- Strategy Economics：现金 PnL、self-excluded PnL、成本、风险、资本效率；
- Scientific Evidence：识别系数、等价性、coverage、OPE 区间和多重检验。

不可变指标原则：

- 指标只读取 ledger/projected state，不修改策略或模拟器；
- 每个指标具有 `metric_id/version/definition/unit/window/input_schema`；
- 币种、tick、bps、quantity、notional 不得共用无单位整数；
- observed、estimated、counterfactual、worst-case、stress 必须显式标记；
- realized 与 mark-to-model 分开；
- micro average、symbol-equal macro、最弱标的和贡献集中度并列；
- 缺失不是零，invalid 不是失败，未识别不是负收益；
- dashboard 仅显示物化视图，不能自行重算另一套口径。

在线安全指标与离线研究指标物理分离。在线安全统计可以影响风险状态，但它必须作为新的 typed evidence event 进入下一次 snapshot；策略不得直接查询 Prometheus 或报表数据库。

## 113. 指标数学口径与反 Goodhart 约束

设冻结账本为 \(L\)，指标定义为版本化纯函数：

\[
M_j=\phi_j^{(v)}(L,\mathcal D_j).
\]

策略训练或选择只可访问 development 指标集合 \(\mathcal M_{dev}\)；最终 lockbox 指标 \(\mathcal M_{test}\) 在冻结前不可见。任何因为看到 test 结果而作出的修改都会生成新 evidence family，并需要新的 lockbox。

综合评分不能替代向量报告。采用 Pareto/词典序判决：

1. 数据和契约有效；
2. 无硬安全违例；
3. 模拟器 fidelity 达标；
4. 科学证据支持；
5. 净经济价值下界为正；
6. 在以上约束内比较增长、资本效率和复杂度。

禁止把安全失败通过更高收益抵消，也禁止不断修改综合权重把失败版本调成第一。

多策略、多标的、多时段搜索使用 family-level alpha/FDR 或 e-value budget；同时报告 probability of backtest overfitting、deflated performance estimate 和选择后置信区间。研究停止规则在 manifest 冻结。

## 114. 实验编排与密封评估

RunManifest 至少包含：

- source/world/strategy/optimizer/schema/metric 版本；
- Git SHA、构建工具链、依赖锁和平台；
- 原始输入 content hash；
- calendar、PriceMode、FX、fee、funding 和合约规则版本；
- random seed 与完整 random tape identity；
- 方法组件图；
- discovery/calibration/validation/lockbox 分割；
- 预注册假设、阈值、停止规则和排除规则；
- 输出目录、父实验和变更理由。

实验编排器只做依赖注入和生命周期管理，不包含撮合、策略或指标公式。同一矩阵先生成 sealed plan，再运行；运行期间不得增删方法。失败 ledger 也必须保留。

参考 AQuA 的值得借鉴之处是 evaluator、数据切分和候选表达式被密封，研究循环只能提交受限变更；AnchorBell 采用同样的“sealed evaluator”思想，但不采用其收益数字作为证据。参考 ABIDES 的 Core/Markets/Gym 分层，将通用离散事件核、市场机制与策略接口分离；AnchorBell 进一步加入交易所规则、证据账本和实时执行一致性。

## 115. 模拟器真实性的分层验收

不得用一个 realism score 宣称模拟器真实。分五层验证：

S0 Protocol：序列、时间、订单状态、价时优先、账户守恒、规则和故障恢复；

S1 Marginal：价差、深度、成交量、事件间隔、撤单率、延迟分布；

S2 Conditional：给定波动、imbalance、PriceMode、time-to-open 后的响应；

S3 Path/Joint：自相关、长记忆、跨标的共跳、fill-markout 联合分布；

S4 Intervention：自身挂单、撤单、改单和规模变化后的反事实响应。

每层均输出通过/条件通过/不通过/未识别。S0 不通过时高层相似度无意义；S4 未识别时只能在 influence bound 内使用 factual replay。

对 EvoMarket、FlowLOB、ABIDES、JAX-LOB 等方案只吸收可验证组件：

- ABIDES：消息驱动离散事件和显式延迟；
- JAX-LOB：大规模并行订单簿推进；
- FlowLOB：条件生成与 counterfactual distribution test；
- EvoMarket：机制 fidelity、微观结构 fidelity、规模与校准联合评价。

不引入与 Binance 合约语义冲突的整套外部模拟器。

## 116. Simulation、Replay、Backtest、Testnet、Live 的关系

五种环境共享 contracts、strategy、optimization、ledger schema 和 metrics definitions，只替换 WorldPort/ExecutionPort：

| 环境 | 市场输入 | 动作结果 | 可作出的结论 |
|---|---|---|---|
| Backtest | 历史聚合数据 | 粗粒度模型 | 仅快速筛除明显失败 |
| Replay | 原始事件流 | factual/structural world | influence 范围内执行估计 |
| Simulation | 实时公开流 | 反事实模拟成交 | shadow 行为和实时稳定性 |
| Testnet | 测试环境真实接口 | 测试交易所确认 | 接口、生命周期、恢复 |
| Live | 生产接口与账户 | 真实成交和现金流 | 唯一真实执行证据 |

不可从 Backtest/Simulation 收益直接宣称 Live edge。环境差异作为 capability manifest 显式列出，缺少某能力时对应指标为 Unavailable，而不是用默认值补齐。

## 117. 高性能边界

正确分层不能以牺牲热路径为代价。采用静态分发、紧凑值对象和批量事件：

- 领域数值使用带单位 newtype，内部定点整数；
- MarketEnvelope 解析一次，通过只读 Arc/slot 引用传播；
- 每标的单写者状态机，跨标的组合层按固定节拍读取一致快照；
- 日志热路径写预分配 WAL/ring buffer，压缩和指标异步投影；
- 策略候选生成与模拟 rollout 可并行，但最终账户风险和组合批准单写者提交；
- 禁止在 tick 热路径动态 JSON、自由字符串、网络查规则或全局锁；
- 配置和规则编译成不可变 runtime snapshot，版本切换使用原子发布；
- 所有优化先测 p50/p95/p99.9 与最坏时限，平均速度不代表可部署。

性能降级顺序固定：减少 challenger rollout → 使用缓存情景 → 使用已验证 incumbent → NO_ACTION/REDUCE。不得通过跳过日志、对账或硬约束换取速度。

## 118. 架构级数学不变量

每次事件和动作后必须保持：

账户守恒：

\[
Equity_t=Cash_t+RealizedPnL_t+UnrealizedPnL_t
-Fees_t-Funding_t.
\]

订单守恒：

\[
Submitted=Rejected+Canceled+Expired+Filled+Open+Unknown,
\]

其中部分成交数量单独满足 quantity conservation。

事件守恒：

\[
Accepted+Dropped+Rejected+Quarantined=Received.
\]

因果完整性：

\[
\forall e\in DecisionLedger,\quad
parents(e)\subseteq SourceLedger\cup WorldLedger\cup DecisionLedger.
\]

策略隔离：

\[
World.step(\cdot)\perp methodId\mid
(state,action,event,randomTape).
\]

指标纯度：

\[
L_1=L_2\Longrightarrow
\phi_j^{(v)}(L_1)=\phi_j^{(v)}(L_2).
\]

实验公平：

\[
ExogenousTape_{m_1}=ExogenousTape_{m_2},
\]

而因动作导致的内生分叉必须被保存，不能强行让不同策略看到同一个已被自身动作影响的未来世界。

这些不变量进入 property-based、metamorphic、fault-injection 和 deterministic replay 测试，而非只写在文档中。

## 119. Round 10 实施序列

A0：冻结 `contracts v1`：单位值对象、MarketEnvelope、DecisionSnapshot、ActionCandidate、ExecutionEvent、EvidenceEvent。

A1：从 `simulation.rs` 提取 WorldPort、StrategyPort、OptimizationPort、LedgerPort；建立 characterization tests，保证拆分前后现有行为可对照。

A2：拆分 factual/structural/generative world；将 FillModel 升级为 ExecutionKernel，不再以 TopOfBook 二值成交为核心抽象。

A3：把 M1–M7 变为组件图和配置；删除 world 对 variant 的任何可见性。

A4：建立四类追加式 ledger、schema registry、stop cause、checkpoint 和 hash-chain；JSONL 变为投影。

A5：建立 metrics registry 和四类 namespace；迁移现有指标并保留旧 metric version。

A6：建立 sealed RunManifest、矩阵计划、random tape 和 lockbox 访问边界。

A7：接入 R8/R9 的 PriceMode、self-excluded book、opening bridge、response set 和证书求解。

A8：完成 S0–S4 模拟器验收、跨环境 contract tests 和全矩阵实验。

顺序不能颠倒为“先实现最复杂策略，再补日志与架构”。A0/A1/A4 是后续数学模型可信的基础。

## 120. Round 10 门禁

- A-G0 Boundary：依赖图无反向边；world 不识别 method，strategy 无 I/O，metrics 不进入决策热路径；
- A-G1 Replay：同输入、动作、random tape 和版本得到字节级一致 ledger；
- A-G2 Conservation：账户、订单、事件和数量守恒在 fault injection 下仍通过；
- A-G3 Evidence：所有决策都有完整 parent lineage，所有停止都有可验证原因；
- A-G4 Metric Stability：同 ledger/metric version 输出一致；新版口径不会覆盖旧结果；
- A-G5 Fairness：公共外生 tape 一致，内生动作分叉正确隔离；
- A-G6 Performance：p99.9 在预算内，降级路径不跳过安全与证据；
- A-G7 Environment Parity：Replay/Simulation/Testnet/Live 使用相同策略与批准动作契约；
- A-G8 Full Matrix：所有方法、标的、PriceMode、world family 与命名故障完整运行。

研究依据：

- [ABIDES public architecture](https://github.com/jpmorganchase/abides-jpmc-public)：通用离散事件 Core、Markets 和 Gym 分离，消息通信显式支持延迟。
- [EvoMarket](https://arxiv.org/abs/2604.18046)：将机制 fidelity、微观结构 fidelity、可扩展性和校准作为联合目标。
- [FlowLOB](https://arxiv.org/abs/2608.13096)：条件生成、跨标的迁移和反事实条件有效性测试。
- [AQuA](https://arxiv.org/abs/2608.12841)：密封 evaluator、固定数据分割和受限候选变更可降低研究循环污染；其报告收益不作为 AnchorBell 的外部证据。
- [History-dependent OPE in POMDPs](https://arxiv.org/abs/2503.01134)：提醒日志覆盖和 revealing 条件不足时，离线评价无法仅靠估计器复杂度解决。

Round 10 的最终边界是：模拟器负责“世界如何响应”，策略负责“提出什么动作”，求解器负责“哪些动作安全且最优”，执行器负责“把批准动作作用于环境”，日志负责“不可变地记录发生了什么”，指标负责“从冻结事实推导什么结论”，实验治理负责“保证比较没有被研究过程污染”。

## 121. Round 11 总目标：实盘等价优先，生存约束内追求超额复合增长

AnchorBell 的目标不是最大化一次 simulation run 的净利润，而是寻找在真实交易所机制、真实可观测信息、真实成交约束和真实资本限制下可持续的超额收益。

目标采用严格词典序：

\[
\text{L0：协议正确、事实完整、账户可恢复；}
\]

\[
\text{L1：在可信黑天鹅集合内满足生存与合规约束；}
\]

\[
\text{L2：在 L0/L1 可行域内最大化长期净复合增长；}
\]

\[
\text{L3：在近似同增长解中最大化稳健 Sharpe/Sortino/Calmar；}
\]

\[
\text{L4：依次最小化最大回撤、尾部损失、换手、模型敏感度和复杂度。}
\]

不存在无条件同时“收益最高、Sharpe 最高、回撤最低”的单一策略。系统必须输出 Pareto frontier、约束影子价格及偏好选择依据。黑天鹅存活也不能解释为任意未知事件中绝不亏损，而是：对明确列出的可信威胁集合给出概率上界、压力损失上界、行动可达性和资本缓冲证书；集合外事件触发 fail-safe，而不是伪造保证。

## 122. 各子系统六元契约

每个子系统 \(X\) 必须以六元组声明：

\[
X=(State,Input,Output,Invariant,Parameters,Metrics).
\]

- State：由谁拥有、如何恢复、版本是什么；
- Input：可读取哪些事实及其时间语义；
- Output：唯一合法输出类型；
- Invariant：任何实现不得破坏的核心；
- Parameters：允许校准、替换或学习的部分；
- Metrics：独立验收方式。

任何参数若会改变 Invariant，就不是参数升级，而是新契约版本。任何指标若被用于下一次决策，必须先转换成有来源、有时间戳的 EvidenceEvent，不能通过旁路共享内存进入策略。

## 123. 模拟器六元契约：真实世界近似，而非收益生成器

### 123.1 State

\[
S_t^{world}=(Book,Trades,Orders,Account,Reference,
Clock,Connectivity,Counterparties,RandomTape).
\]

### 123.2 Input/Output

输入仅为外生 SourceEvent、标准化 ApprovedAction 和显式 FaultEvent；输出仅为 WorldEvent 与新 WorldState。模拟器不得读取策略内部 belief、目标收益或指标排名。

### 123.3 不可变核心

- 交易所状态机与账户守恒；
- 单一离散事件因果顺序；
- 价格—时间优先及 Unknown 状态；
- 自身动作进入反事实世界；
- 同版本确定性回放；
- 不因 method_id 改变世界；
- 所有未建模区域显式标记。

### 123.4 可变部分

- 外生潜在价值过程；
- 异质交易者和订单流 family；
- 队列消耗/撤单位置模型；
- 延迟、断线和系统故障分布；
- Impact Mid/EWMA 未公开参数集合；
- 市场冲击和恢复动力学；
- 生成式路径模型。

### 123.5 校准目标

模拟器参数 \(\theta\) 不以策略 PnL 为损失，而以机制统计向量 \(T\) 校准：

\[
\hat\theta=
\arg\min_\theta
\sum_g w_g\,d_g
\left(T_g^{real},T_g^{sim}(\theta)\right)
+\lambda\Omega(\theta).
\]

\(g\) 覆盖 protocol、marginal、conditional、joint、intervention、tail。由于多组参数可能产生相近统计量，必须输出 identified set：

\[
\Theta_{cal}=
\{\theta:L(\theta)\le L(\hat\theta)+\delta\},
\]

策略在整个 \(\Theta_{cal}\) 上评估，而不是只使用最佳拟合点。

### 123.6 模拟器指标

除 Round 10 的 S0–S4 外，新增 posterior predictive rank、parameter sloppiness、profile likelihood、simulation-based calibration、intervention coverage 和 strategy-rank stability。模拟器能复现价格分布但不能复现成交后 markout 时仍不合格。

## 124. 策略六元契约：识别 edge 并提出动作

### 124.1 State

\[
S_t^{policy}=(Belief_t,EvidenceState_t,
InventoryPreference_t,Eligibility_t).
\]

### 124.2 Input/Output

输入是冻结 DecisionSnapshot；输出是零个或多个 ActionCandidate、候选条件价值分布、reason code 和 belief update。策略不输出“必须成交”的命令。

### 124.3 不可变核心

- 官方收盘锚点不可被合约反写；
- 合约偏离不等于必然错价；
- 闭市 Index/Mark 的内生性必须剔除；
- 只使用 known-at-time 信息；
- 证据不足允许拒绝交易；
- 所有动作先经过独立安全和求解层；
- 正常执行以 Maker/Post-only 为核心。

### 124.4 可变部分

- 潜在公平价滤波器；
- informed/uninformed order-flow belief；
- 跳跃/制度状态；
- evidence cell 定义；
- entry/exit/cancel 候选生成；
- inventory skew；
- deadline 与 funding-aware 逻辑；
- 可解释 challenger 特征。

### 124.5 信息状态模型

把闭市参与者分为 informed \(Z_t=1\) 与 uninformed/fad \(Z_t=0\) 的隐状态混合：

\[
P(Z_t=1\mid\mathcal I_t)=\pi_t.
\]

订单流观测强度：

\[
\lambda_k(t\mid Z_t)
=\lambda_{0,k}^{(Z_t)}
+\sum_j\int g_{kj}^{(Z_t)}(t-s)dN_j(s).
\]

若单边主动成交、跨市场共同因子、新闻跳跃和成交后持续 markout 同时增强，则 informed posterior 上升，均值回归规模下降。若偏离主要由低深度、短暂队列失衡且外部信息弱，则 fad/microstructure posterior 上升。

策略交易的不是原始 residual，而是：

\[
m_t=p_t-E[f_t\mid\mathcal I_t],
\]

以及净可实现优势：

\[
Edge_t(a)=
E[\mathbf1_{fill(a)}(Y_{liq}-p_{fill})
-C_{all}(a)\mid\mathcal I_t].
\]

### 124.6 策略指标

预测校准、方向命中、conditional edge、evidence monotonicity、abstention quality、信息状态识别、候选覆盖率和 realized-vs-predicted net edge。Sharpe 不是策略模型训练的唯一目标。

## 125. 日志六元契约：现实发生过什么的唯一权威

### 125.1 State

每条 stream 维护 sequence、prev_hash、writer epoch、durable offset、schema version 和 checkpoint lineage。

### 125.2 双时间与修订语义

事件至少具有：

\[
(validTime,knownTime,receiveTime,commitTime).
\]

- validTime：事件声称在现实中何时生效；
- knownTime：系统最早何时有权知道；
- receiveTime：本机何时收到；
- commitTime：何时确保持久化。

企业行动、交易日历和官方收盘可能事后修订。原始记录不覆盖，而追加 RevisionEvent：

\[
Revision=(targetEventId,oldHash,newPayload,
effectiveFrom,knownAt,reason,source).
\]

回放必须支持 as-known-at 与 latest-revised 两种视图；策略评价只允许 as-known-at，数据质量研究可比较两者。

### 125.3 不可变核心

追加式、可验证持久化、因果父链、单位明确、失败路径完整、停止原因完整、secret 结构化剔除。

### 125.4 可变部分

WAL 格式、分片大小、压缩、索引、Parquet 投影、冷热存储和查询引擎。

### 125.5 日志指标

durable lag、sequence gap、hash mismatch、correction rate、known-time violation、orphan causation、flush completeness、replay divergence 和 schema migration coverage。

## 126. 指标六元契约：事实、估计和决策价值不能混淆

每个指标输出：

\[
MetricValue=(estimate,unit,status,window,
validTime,knownTime,methodVersion,
uncertainty,lineage).
\]

status 至少包括 Observed、Estimated、Counterfactual、Stress、WorstCase、Invalid、Unavailable。

### 126.1 收益指标

- realized cash PnL；
- self-excluded marked PnL；
- gross alpha；
- maker rebate/fee；
- funding；
- adverse selection；
- opportunity cost；
- emergency exit；
- model/solver error reserve；
- total net economic PnL。

### 126.2 风险指标

- peak-to-trough maximum drawdown；
- duration and recovery time；
- expected shortfall；
- drawdown-at-risk；
- ruin/liq upper confidence bound；
- gap-open loss；
- liquidity-adjusted exposure；
- unresolved-order exposure；
- self-reference feedback exposure。

### 126.3 风险调整收益

普通 Sharpe：

\[
SR=\frac{E[r_t-r_f]}{\sqrt{Var(r_t-r_f)}}
\]

必须同时给出自相关与异方差修正、block-bootstrap 区间及 deflated/selection-adjusted 版本。小样本高 Sharpe 不作为上线依据。

同时报告：

\[
Sortino=\frac{E[r-r_f]}{\sqrt{E[\min(r-r_f,0)^2]}},
\]

\[
Calmar=\frac{AnnualizedReturn}{MaxDrawdown},
\]

以及长期 log-growth、Omega、tail ratio、profit factor，但不压成一个不可解释分数。

### 126.4 不确定性传播

总结果不确定性至少拆为：

\[
Var(V)=
E[Var(V\mid\theta,M,D)]
+Var(E[V\mid\theta,M,D]),
\]

进一步分成路径随机性、参数、模型形式、数据修订、执行、指标估计和 solver gap。报告必须给每类贡献，避免用窄 bootstrap 区间掩盖模型不确定性。

## 127. 黑天鹅威胁模型

黑天鹅测试不能只把波动率乘三。建立组合威胁图：

- 股票真实价值跳跃与币安订单簿薄化同时发生；
- Index/Mark 模式切换叠加 FX 断流；
- 多标的共同跳跃导致相关性趋近一；
- Maker 单选择性成交，减仓单长期不成交；
- 撤单超时、Unknown orders 与私有流中断；
- API 限频、时钟漂移、磁盘阻塞和进程暂停；
- 开盘涨跌停、停牌、企业行动或延迟；
- USDT 偏离、保证金规则变化；
- 对手识别策略后流动性撤离；
- 交易所临时调整价格保护或合约规格。

对威胁组合 \(c\) 定义严重度、先验下界、可观测预警、恢复动作和不可恢复损失。使用 fault tree/attack graph 枚举共同原因，而不是假定故障独立。

生存条件：

\[
\sup_{Q\in\mathcal U^{tail}}
P_Q(W_T<W_{floor})\le\epsilon_{ruin},
\]

\[
\sup_{Q\in\mathcal U^{tail}}
ES_\alpha(-R_T)\le L_{ES},
\]

\[
\inf_{Q\in\mathcal U^{tail}}
P_Q(\text{safe recovery by }T)\ge1-\epsilon_{rec}.
\]

零观测失败仍使用上置信界，不报告零风险。无法为某个组合校准概率时，将其作为命名确定性 stress，要求损失不超过预设资本预算。

## 128. 安全域内的多目标最优组合

定义净收益向量 \(R(q,\omega)\)，决策变量为离散报价和仓位。首先求安全可行域：

\[
\mathcal F=
\{q:
MarginSlack^\omega(q)\ge0,\
Drawdown^\omega(q)\le d_{hard},\
ReferenceImpact^\omega(q)\le i_{hard},\
\forall\omega\in\Omega_{credible}\}.
\]

然后在 \(\mathcal F\) 内求 Pareto 前沿：

\[
\max_{q\in\mathcal F}
\left(
g_{robust}(q),
SR_{LCB}(q),
Calmar_{LCB}(q),
-ES^{upper}(q),
-MDD^{upper}(q)
\right).
\]

生产决策不直接对 Sharpe 的比率做不稳定优化，而使用等价的参数化/二阶锥形式。例如对目标波动 \(\sigma^\star\)：

\[
\max_q\underline\mu^\top q-C(q)
\]

subject to

\[
\|Lq\|_2\le\sigma^\star,
\]

再扫描 \(\sigma^\star\) 构造有效前沿。对 log-growth 使用鲁棒情景平均并限制下尾；对最大回撤使用 path-wise auxiliary variables 和嵌套风险约束。

选择规则：

1. 生存约束不通过直接淘汰；
2. \(Adv_{cert}\le0\) 选择 NO_ACTION；
3. 在剩余解中选 robust log-growth 最大；
4. 若增长置信区间重叠，选回撤和模型敏感度更低者。

## 129. 多时间尺度最优控制

现实系统不能在每个 tick 重解完整 POMDP。采用五层控制频率：

| 层 | 典型尺度 | 责任 |
|---|---:|---|
| L0 Event | 微秒–毫秒 | 解析、排序、盘口、订单生命周期 |
| L1 Safety | 每事件/毫秒 | 硬约束、撤单竞态、Unknown、紧急状态 |
| L2 Quote | 50–500ms | 候选报价、queue persistence、局部 MPC |
| L3 Portfolio | 1–60s | 跨标的资本、因子暴露、风险储备 |
| L4 Validation | session/day | belief 校准、模型集合、证据判决 |

离线层求 PIDE/viability、terminal opening value、情景 cuts 和 response families；中频层更新 belief 与 risk envelopes；在线层只执行候选枚举和小型锥/整数优化。

层间只通过版本化 snapshot 交换。慢层更新不能直接修改正在执行的快层状态，而发布新 RuntimePolicySnapshot，在安全边界时原子切换。紧急安全层具有最高优先级，但每次覆盖都必须形成日志事件。

## 130. 模拟器—策略—日志—指标的因果隔离实验

为证明边界真实有效，建立四组 metamorphic tests：

### 130.1 模拟器无策略身份

保持动作序列、事件和 random tape 不变，仅改变 method label，World Ledger 必须完全一致。

### 130.2 策略无环境身份

保持 DecisionSnapshot 与配置不变，在 replay/simulation/live harness 中调用，ActionCandidate 必须完全一致。

### 130.3 日志无指标反作用

增加、删除或升级离线指标投影，Source/World/Decision/Account Ledger 必须完全一致。

### 130.4 指标可复现

对相同 sealed ledgers 和 metric version，在不同运行顺序与并行度下结果一致；浮点指标使用确定性聚合或明确误差界。

另外执行 policy-shift test：当策略动作分布离开模拟器校准支持时，intervention coverage 必须下降并触发 Unidentified，而不能保持虚假高置信度。可控生成模拟研究表明，缺乏 policy-conditioned 因果一致性时，评价方差可能在策略迁移下快速放大，因此该门禁是不可变核心。

## 131. 实盘一致性的证据阶梯

上线证据按不可跳级的阶梯累积：

E0 Contract tests：交易所规则、时间、单位和状态机；

E1 Historical replay：同一 raw event 可确定回放；

E2 Shadow simulation：实时公开流、无真实动作；

E3 Testnet/interface canary：真实 API 生命周期与恢复；

E4 Minimum-size live probe：极小 Maker 动作校准 queue/latency/response；

E5 Capital canary：严格风险预算内小资本闭环；

E6 Controlled scale-up：只有证据下界、coverage 和 tail certificate 连续达标才扩容。

每一级都必须输出 capability gap。Testnet 不能证明 production liquidity；Simulation 不能证明 fill；Live probe 不能单独证明长期 alpha。扩容函数必须连续且有迟滞：

\[
Capital_{t+1}\le
Capital_t+
\gamma\,[EvidenceLCB_t-E_{min}]_+,
\]

而风险恶化允许立即非对称收缩。

## 132. Round 11 实施切片与门禁

实施切片：

B0：为 world、strategy、ledger、metrics、optimization 写六元契约和 Rust trait/type；不搬业务代码前先冻结边界。

B1：建立 bitemporal/source-time 类型、RevisionEvent 和 as-known-at replay。

B2：实现 SimulatorCalibrationSpec、identified parameter set 和 S0–S4 分层结果。

B3：实现 StrategySnapshot、隐含 informed/fad belief 和净 edge candidate。

B4：建立 MetricRegistry、单位系统、四类指标 namespace 和不确定性分解。

B5：实现 ThreatGraph、组合故障生成和 tail certification。

B6：实现多时间尺度 runtime snapshot、原子版本切换和安全覆盖日志。

B7：实现 Pareto frontier 离线扫描及在线目标波动 SOCP/整数候选求解。

B8：完成因果隔离 metamorphic tests 与 E0–E4 证据阶梯；之后才恢复策略收益实验。

门禁：

- B-G0：六元契约完整且依赖方向静态检查通过；
- B-G1：as-known-at 回放不存在未来修订泄漏；
- B-G2：模拟器参数集合而非单点支持策略结论；
- B-G3：策略在 informed posterior 上升时风险单调不增；
- B-G4：所有 PnL/risk/Sharpe 指标单位、状态和区间完整；
- B-G5：组合黑天鹅下仍满足预注册资本底线，或明确拒绝部署；
- B-G6：不同时间尺度切换无半版本 snapshot；
- B-G7：Pareto 解全部在安全域，独立 verifier 通过；
- B-G8：实盘证据阶梯没有跳级和能力冒充。

研究依据与采用边界：

- [Market Making with Fads, Informed, and Uninformed Traders](https://arxiv.org/abs/2501.03658)：支持在部分信息下从匿名订单流推断短暂偏离与信息交易者；其市场假设需针对 Binance 校准。
- [Market Informedness and Market-Maker Profitability](https://arxiv.org/abs/2606.05882)：支持异质做市商、内生价格和状态相关自激订单流的 stress family。
- [Optimal Execution with Passive Market Impact](https://arxiv.org/abs/2607.28323)：支持把被动成交概率随距离衰减和被动冲击纳入执行控制。
- [EvoMarket](https://arxiv.org/abs/2604.18046)：支持机制、微观结构和可扩展性联合验证。
- [SHIELD](https://arxiv.org/abs/2605.09171)：支持用可验证筛选减少凸优化变量与约束；只能在其证明条件满足时用于加速。
- [Conformal early stopping for MIP](https://arxiv.org/abs/2602.01476)：可作为 solver 早停 challenger；生产安全仍以确定性可行 incumbent 和独立 verifier 为准。
- [Controllable simulation under policy shift](https://arxiv.org/abs/2605.11519)：支持将策略条件和干预一致性作为反事实模拟核心，防止 policy shift 下 controllability collapse。
- [Rigorous walk-forward validation](https://arxiv.org/abs/2512.12924)：支持严格时序样本外、固定参数和防前视验证；作为研究治理参考而非收益证据。

Round 11 至此把“高度真实”和“最优”变成可检验定义：高度真实是协议、条件分布、路径联合、干预响应和实盘能力逐层对齐；最优是在已声明信息集、可信模型集合、黑天鹅威胁集、离散动作和实时预算内，先可生存、再最大化有证书的净复合增长，并报告 Sharpe/回撤的完整 Pareto 权衡。

# Round 12：把固定锚均值回归核心假设变成可证伪实验

## 133. 本轮直接结论与不可偷换的语义

AnchorBell 的研究核心继续保持：

> 在 A 股或港股底层市场休盘期间，最近一个合格官方收盘价形成外部固定锚；Binance TradFi 永续相对该锚的部分偏离可能是暂时性的，并可能产生可交易的条件均值回归。

这里的“保持不变”指研究问题、锚的来源和禁止未来信息不变，不是预先宣布结果必然为真。系统必须允许最终结论为拒绝、未识别、仅条件成立或经济上不可交易。

2026-05-16 之后必须区分：

| 对象 | 休盘期是否固定 | 是否受 Binance 订单簿影响 | 研究角色 |
|---|---:|---:|---|
| 底层股票本币官方收盘价 \(A_e^{native}\) | 是，除正式调整事件 | 否 | 外部固定锚 |
| 合约口径锚 \(A_{e,t}^{contract}\) | Quanto 通常数值固定；FX 型可能变化 | FX 间接变化 | 可交易计价基准 |
| Binance Impact Mid | 否 | 是 | 内生订单簿量 |
| Binance Price Index \(I_t\) | 否，Orderbook-EWMA | 是 | 保证金、Mark、Funding 参考 |
| Binance Mark Price \(M_t\) | 否 | 是 | 风险和清算参考 |
| 合约成交价/盘口 \(P_t\) | 否 | 是 | 实际交易对象 |

Binance 官方公告确认，股票类 TradFi 永续在维护期、周末和节假日已由 Fixed Mode 改为 Orderbook EWMA。官方 FAQ 进一步说明，休盘期 Index 使用订单簿 Impact Mid、EWMA 平滑和运动限制；股票合约在周末和节假日的 Mark/Index 偏离约束为正负 3%。这些规则约束的是合约与 Binance 内生 Index 的关系，不自动证明合约会回到股票官方收盘价。

因此冻结两个不同命题：

- Core Validation ValidationClaim：固定外部锚附近存在可重复的条件恢复力；
- Exchange Mechanism ValidationClaim：Binance 规则是该恢复力的重要因果来源。

第一项是项目核心，第二项必须单独识别，不能由第一项的相关性结果推出。

## 134. 锚定义、计价和代数分解

对标的 \(i\)、休盘事件 \(e\)，定义合格本币锚：

\[
A_{i,e}^{native}=Close_{i,e}^{official,as-known-at}.
\]

它只可被预先声明的企业行动、除权除息、合约乘数变化或官方纠错事件变换。任何 Binance 合约价格、Index、Mark、策略信号或未来开盘价都不得反向修改它。

合约口径锚由纯映射层产生：

\[
A_{i,e,t}^{contract}
=\mathcal T_i(A_{i,e}^{native},FX_t,Multiplier_t,Adjustment_t).
\]

Quanto 合约必须保留 \(1\ local\ currency=1\ USDT\) 的合约约定；USDT-priced 港股和使用 USDCNH 的中国股票合约必须分别记录实时 FX 变换。由此区分“本币锚固定”和“USDT 数值完全不动”，防止把汇率变化误判成股票偏离。

至少同时维护四个对数基差：

\[
b_t^{P/A}=\log P_t-\log A_t,\quad
b_t^{I/A}=\log I_t-\log A_t,
\]

\[
b_t^{P/I}=\log P_t-\log I_t,\quad
b_t^{M/I}=\log M_t-\log I_t.
\]

存在精确恒等式：

\[
b_t^{P/A}=b_t^{P/I}+b_t^{I/A}.
\]

因此：

\[
\Delta b_t^{P/A}
=\Delta b_t^{P/I}+\Delta b_t^{I/A}.
\]

若合约与 Index 的偏离缩小，但 Index 本身被合约订单簿带离官方锚，则不能声称发生了“规则驱动的向固定锚回归”。所有报告必须展示这两个分量，不准只画 \(P/A\) 一条线。

价格观测分为 last、top-of-book mid、microprice、Impact Mid、可成交买价、可成交卖价。统计假设的主价格预注册为 self-excluded microprice；经济假设只允许使用方向正确的可成交价格和真实 maker fill。

## 135. 六级假设栈

### 135.1 H-A：锚完整性

检验休盘事件内 \(A^{native}\) 是否保持不变，以及所有变化是否都有先验允许的 RevisionEvent 或 AdjustmentEvent。

\[
H_A:\quad \Delta A_{e,t}^{native}=0
\]

适用于事件内部所有非调整时刻。H-A 失败时，该事件不得进入均值回归样本。

### 135.2 H-R：统计均值回归

令 \(b_t=b_t^{P/A}\)，对预注册预测跨度 \(h\) 定义朝锚改善量：

\[
G_{t,h}=-\operatorname{sgn}(b_t)(b_{t+h}-b_t).
\]

\(G_{t,h}>0\) 表示向锚靠近。核心检验不是“最终是否碰过锚”，而是：

\[
H_{0,R}:E[G_{t,h}\mid \mathcal E_t]\le \delta_{stat},
\qquad
H_{1,R}:E[G_{t,h}\mid \mathcal E_t]>\delta_{stat}.
\]

\(\mathcal E_t\) 是预先冻结的合格状态集合；\(\delta_{stat}\) 是最小统计相关效应，而不是零。

### 135.3 H-M：Binance 机制归因

检验 Funding、Mark/Index 偏离限制、Orderbook-EWMA 和模式切换是否产生可识别恢复力。要求在控制公开信息和订单流后，规则暴露对 \(P/I\)、\(I/A\) 及 \(P/A\) 的作用方向和时序符合机制预测。

仅观察到 \(P/A\) 下降不足以接受 H-M。必须排除共同订单流、自身报价进入 Impact Mid、时间趋势、开盘临近预期和外部信息变化。

### 135.4 H-D：偏离是暂时噪声而非价格发现

下一次底层市场恢复交易后的稳健价格 \(Y_e\) 是事后标签。检验 Binance 休盘偏离是在回归旧锚，还是提前发现下一开盘价值。

若 \(P_t\) 比 \(A_e\) 更接近 \(Y_e\)，且偏离方向持续预测开盘跳空，则向旧锚反向交易可能是在对抗真实信息。

### 135.5 H-E：可执行经济优势

在 maker-only、真实排队、部分成交、逆向选择、手续费、Funding、FX、未成交机会成本和强制退出成本后：

\[
H_{0,E}:LCB_\alpha(E[PnL^{net}_{trade}])\le0,
\]

\[
H_{1,E}:LCB_\alpha(E[PnL^{net}_{trade}])>0.
\]

统计均值回归成立但 H-E 失败时，策略仍不得上线。

### 135.6 H-S：资本与尾部生存

要求在预注册联合黑天鹅、开盘跳跃和流动性消失场景下，破产概率、Expected Shortfall、最大回撤和恢复失败上界全部处于资本预算内。H-S 优先级高于收益和 Sharpe。

## 136. 与现实一致的潜在价值—暂时偏离模型

固定收盘锚不是休盘期间不可变化的真实经济价值。引入潜在有效价值 \(V_t\) 和暂时微观结构误差 \(U_t\)：

\[
\log P_t=\log V_t+U_t+\epsilon_t^{micro}.
\]

有效价值吸收休盘新闻、ADR/OTC、行业指数、期货、汇率和全球风险因子：

\[
d\log V_t
=\beta_{S_t}^{\top}dZ_t+\sigma^V_{S_t}dW_t^V+dJ_t^V.
\]

暂时偏离允许非线性、非高斯、异方差和状态切换：

\[
dU_t
=-\kappa_{S_t}(U_t)\,dt
+\sigma^U_{S_t}(U_t)dW_t^U+dJ_t^U.
\]

其中 \(S_t\) 至少包括 closure type、Orderbook-EWMA mode、流动性、新闻、Funding 邻域、临近开盘、拥挤和异常状态。允许死区和非对称恢复：

\[
\kappa_s(u)=
\begin{cases}
0,& |u|\le c_s,\\
\kappa_s^+,&u>c_s,\\
\kappa_s^-,&u<-c_s.
\end{cases}
\]

Binance Index 不是外生状态，而是订单簿的反馈函数：

\[
I_t=\operatorname{CapMove}\left(
EWMA_{\lambda_s}(ImpactMid(LOB_t))
\right).
\]

合约订单流强度使用带标记点过程：

\[
\lambda_t^k
=\mu_{S_t}^k+
\sum_j\int_0^t\phi_{kj,S_t}(t-u)dN_u^j,
\]

以表达成交、撤单、盘口移动和信息事件的自激与交叉激励。策略订单必须作为单独 marked intervention 进入 \(LOB_t\)，不得从世界中消失。

这组模型承认三种现实可能：

1. \(V_t\approx A_e\)，\(U_t\) 暂时偏离并回归，核心策略有机会；
2. \(V_t\ne A_e\)，合约在做有效价格发现，盲目回归会亏损；
3. \(I_t\) 与 \(P_t\) 自反馈共同漂移，表面稳定但没有外部锚恢复力。

## 137. 不依赖单一 OU 的检验族

主结果采用多跨度局部投影：

\[
b_{i,e,t+h}-b_{i,e,t}
=\alpha_{i,h}+\eta_{e,h}
+\beta_h b_{i,e,t}
+\gamma_h^\top X_{i,e,t}
+\varepsilon_{i,e,t,h}.
\]

均值回归要求 \(\beta_h<0\)，同时要求经济幅度超过预注册阈值。推断按 symbol 与 closure episode 双向聚类；样本簇少时使用 wild cluster bootstrap。

同时估计非参数恢复面：

\[
R_h(b,x)
=-E[\operatorname{sgn}(b)\Delta_hb\mid b,x]/h.
\]

只有在可交易偏离区域的下置信带大于零，才可声称存在恢复力。中心死区允许 \(R_h\approx0\)。

建立以下 challenger family：

- M0：带跳跃随机游走，无恢复；
- M1：线性 OU；
- M2：阈值 TAR/ECM，允许死区与上下不对称；
- M3：Markov switching jump-diffusion；
- M4：非线性、非高斯状态空间模型；
- M5：订单流 Hawkes 与 queue-reactive 模型；
- M6：局部投影和单调约束非参数模型。

模型选择只使用冻结验证集上的 predictive log score、calibration 和机制统计，不使用策略 PnL。若模型对恢复方向产生实质分歧，结果标记 Model-Ambiguous，并将参数集合传给鲁棒优化器。

只有在 \(\kappa>0\) 被识别后才报告半衰期：

\[
t_{1/2}=\log 2/\kappa.
\]

对接近单位根、阈值、跳跃和有限事件窗口，不允许用普通 OLS 半衰期伪精确。ADF 或方差比检验只能作为诊断，不能单独证明可交易均值回归。

## 138. 首达时间、未回归和开盘竞争风险

定义进入时刻 \(t_0\)，净成本覆盖带 \(\epsilon_{cost}\)，止损带 \(B_{stop}\)：

\[
\tau_{anchor}=\inf\{t>t_0:|b_t|\le\epsilon_{cost}\},
\]

\[
\tau_{stop}=\inf\{t>t_0:|b_t|\ge B_{stop}\},
\qquad
\tau_{open}=T_e^{open}.
\]

这是 competing-risks 问题。必须估计：

- \(P(\tau_{anchor}<\tau_{stop}\wedge\tau_{open})\)；
- 条件首达时间分布和尾部，而非只有平均半衰期；
- 未回归概率与 right-censoring；
- 越过锚后的 overshoot、反弹和二次风险；
- 从信号出现到真正 maker fill 后的剩余恢复量。

若只保留成功回归样本，会形成终点选择偏差。所有未成交、未回归、被止损和到开盘仍持有的路径必须留在分母中。

## 139. Price Discovery 反证：旧锚是否已经过时

对每个 closure episode 定义下一开盘外部目标：

\[
Y_e=RobustVWAP([T_e^{open}+\Delta_0,T_e^{open}+\Delta_1]).
\]

窗口、数据源和异常规则必须预注册。定义信息发现增益：

\[
DG_t=|A_e-Y_e|-|P_t-Y_e|.
\]

若 \(DG_t>0\)，休盘合约比旧锚更接近下一开盘。再定义方向一致性：

\[
DC_t=\operatorname{sgn}(P_t-A_e)
\operatorname{sgn}(Y_e-A_e).
\]

当大偏离状态下 \(DG_t\) 的下置信界为正且 \(P(DC_t=1)\) 显著高于基线，说明偏离至少部分是信息，不应整体做反向均值回归。

\(Y_e\) 只准用于离线标签、平滑器和研究归因。任何 online StrategySnapshot 中出现未来 \(Y_e\)、开盘 VWAP 或事后新闻时间戳，均为不可恢复的数据泄漏。

策略真正应交易的是：

\[
U_t=\log P_t-E[\log V_t\mid\mathcal F_t],
\]

而不是未经分解的 \(P_t-A_e\)。固定锚仍是强先验中心，但当信息跳跃后验升高时，锚权重必须下降或策略 abstain。

## 140. Orderbook-EWMA 的因果识别

### 140.1 规则切换准实验

2026-05-16 是股票类 TradFi 永续从 Fixed Mode 转为 Orderbook-EWMA 的明确制度断点。以该日期建立版本化 RuleRegime，不得把断点前后数据直接混合。

估计动态事件研究：

\[
Y_{i,e,t}
=\alpha_i+\delta_e+
\sum_{k\ne-1}\theta_k
1\{eventTime=k\}
+\Gamma^\top X_{i,e,t}+u_{i,e,t}.
\]

结果变量分别取 \(G_{t,h}\)、\(P/I\) 恢复、\(I/A\) 漂移、Funding premium、深度和开盘误差。必须检查前趋势、组成变化和同步市场冲击。

由于制度切换不是随机实验，结论默认标为 quasi-causal。只有处理前拟合、安慰剂日期、负控制结果和敏感性界均通过，才升级机制证据等级。

### 140.2 日内模式切换

利用 Regular、Fast-Decay EWMA、Slow-Decay EWMA、Orderbook-EWMA 的已知边界做窄窗事件研究。切换过渡窗口单独建模，不把平滑过渡误判成自然均值回归。

### 140.3 Funding 的局部作用

Funding Premium 基于 \(P/I\)，因此 Funding 事件首先检验合约向 Index 的恢复，而非直接检验合约向官方 Close 的恢复。

围绕预定 Funding 时间比较同号偏离的局部投影、订单流和持仓变化，并用虚假 Funding 时间作 placebo。若只有 \(P/I\) 收敛而 \(I/A\) 不收敛，则结论只能是“规则锚定内生 Index”。

### 140.4 自反馈污染

由于 \(I_t\) 来自 Impact Mid，参与者订单可能同时改变 \(P_t\) 与 \(I_t\)。实验记录 self-volume share、self-depth share、距 Impact Notional 的比例和反事实删单盘口。

公共行情观察实验不下单，是 H-R/H-D 的主要证据；最小实盘探针只校准成交与自身影响，不用于证明市场天然回归。禁止为制造 Index 变化而摆单、撤单或成交。

## 141. 样本单位、预注册与统计功效

统计独立性的主单位是 closure episode，不是 tick。一个周末的数百万 tick 不能冒充数百万独立样本。

EpisodeKey 至少包含：

\[
(symbol,contractType,closureType,closeTime,
openTime,ruleVersion,anchorVersion).
\]

closureType 必须分开报告：

- 午间休市；
- 普通隔夜；
- 周末；
- 单日节假日；
- 多日长假；
- 临时停牌或异常闭市。

A 股、港股 Quanto、港股 FX 型也不得默认合并。层级模型可以 partial pooling，但必须报告每个 symbol-regime-cell 的后验和覆盖度。

主假设、主跨度、偏离阈值、排除条件、损失函数、最小经济效应、置信水平和停止规则在看结果前写入带哈希 PreregistrationManifest。

所需 episode 数由可检测效应和簇内相关决定：

\[
N_{eff}
\ge
\frac{(z_{1-\alpha}+z_{1-\beta})^2
\sigma_{cluster}^2}{\delta_{min}^2}.
\]

若达不到预注册功效，只能输出 Inconclusive，不能用更多 tick 或更换窗口补显著性。

持续采集期间使用 anytime-valid e-process 或 alpha-spending；普通 p 值不得被每天反复查看后择时停止。多 symbol、方向、跨度和阈值使用 Romano-Wolf stepdown 或预注册 FDR 控制。

## 142. 数据证据包

每个 episode 必须封存下列 raw streams 和 provenance：

- 官方底层交易所日历、休市状态、收盘价、公司行动和修订；
- 合约规格、乘数、quanto/FX 类型、tick/lot、杠杆和保证金版本；
- Binance depth diff、周期快照、bookTicker、aggTrade 和成交方向；
- Price Index、Mark Price、Premium Index、Funding rate/time；
- Index calculation mode、切换边界、过渡窗口、偏离限制和公告版本；
- USDT、HKD/USD、USDCNH 及适用 FX；
- ADR/OTC、行业/国家指数、相关期货和宏观风险代理；
- 新闻事件的 provider timestamp、receiveTime 和 knownTime；
- 本策略订单、ACK、排队估计、成交、撤单、拒单和 Unknown 状态；
- 网络延迟、序列缺口、重连、时钟误差和数据完整性。

所有 raw event 追加写入 Source Ledger；派生 anchor、basis、state、label 和 metric 分别进入 World、Decision、Run 和 Metric Ledger。原始值禁止覆盖。

缺少 Index mode 或 anchor lineage 的 episode 可保留为数据质量样本，但不得进入核心结论。

## 143. 模拟器、实验、策略、日志和指标的严格分工

| 子系统 | 可以做什么 | 不可以做什么 |
|---|---|---|
| Simulator | 重放和生成给定动作下的市场、排队、成交、冲击和故障 | 用策略 PnL 调参；预设 H-R 为真 |
| ValidationClaim Lab | 冻结样本、执行检验、估计效应与不确定性 | 发交易单；修改策略阈值 |
| Strategy | 仅用当时信息估计 \(V_t,U_t\) 并产生候选动作 | 读取未来开盘标签；改写锚 |
| Execution | 执行 maker-only 生命周期和安全退出 | 决定研究是否成立 |
| Logger | 保存因果事实、版本、时钟和修订 | 用聚合统计覆盖原始事件 |
| Metrics | 从 sealed ledgers 计算机制、经济和风险结果 | 反向影响同一实验动作 |
| Optimizer | 在已支持模型与安全域中选择参数 | 把未识别状态当成零风险 |

Simulator 的 H0 世界必须包含无均值回归、纯信息跳跃、内生 Index 跟随、虚假回归和成交选择偏差。若 ValidationClaim Lab 在 H0 世界中频繁接受 H-R，实验本身不合格。

策略版本与实验检验版本分别冻结。研究阶段不得因为中间收益不好而改信号阈值；改动必须创建新的 family member 并消耗多重检验预算。

## 144. 判定矩阵：假设到底算不算成立

每个 symbol-regime-cell 输出以下状态之一：

| 状态 | 含义 |
|---|---|
| Rejected | 方向错误或实际效应低于最小阈值 |
| Inconclusive | 功效、覆盖或数据质量不足 |
| Descriptive-Supported | H-A、H-R 通过，但机制未识别 |
| Mechanism-Supported | H-M 也通过，能区分 \(P/I\) 与 \(I/A\) |
| Conditional-Alpha | H-D 表明只在命名状态中是暂时偏离 |
| Economically-Tradable | H-E 在封存样本和真实执行下净优势下界为正 |
| Deployable | H-S 通过且证据阶梯满足资本等级 |

最小经济阈值不是任意 10bps 或 100bps，而由方向、规模、时间和状态共同决定：

\[
\delta_{econ}(a,t,h)
=C_{fee}+C_{funding}+C_{queue}
+C_{adverse}+C_{unwind}+R_{tail}+R_{model}.
\]

只有：

\[
LCB_\alpha(E[G_{fill,h}\mid a,t])
>
UCB_\alpha(\delta_{econ}(a,t,h))
\]

且成交概率下界、开盘前退出概率下界和资本约束同时通过，才存在可执行优势。

核心假设的研究级总体结论采用预注册覆盖规则：

\[
Coverage=
\frac{\sum_c w_c1\{H_R\ supported\}}
{\sum_c w_c}.
\]

权重由事前业务暴露或等权确定，不得按事后收益加权。必须同时报告最差 cell；总体通过不能掩盖某个标的持续反向。

“币安规则会让价格向锚均值回归”的完整表述只有在 H-A、H-R、H-M 同时通过时成立；H-D 决定何时不应交易；H-E 与 H-S 决定它是否是策略优势。

## 145. 反事实和安慰剂检验

至少执行以下 falsification suite：

1. 随机替换为前两日或后两日收盘锚；真实锚必须显著优于伪锚；
2. 时间反转测试；恢复方向不得在反向时间同样强；
3. 随机平移 Funding 时间；真实事件效应必须优于 placebo；
4. 随机平移 mode-switch 日期；2026-05-16 断点不得由普遍时间趋势解释；
5. 用未来开盘方向分层，检验信息状态是否解释“未回归”；
6. 将 Binance Index 当锚与官方 Close 当锚分别检验，禁止混名；
7. 从 Impact Mid 反事实删除自身挂单，测量 self-reference；
8. 用不可能成交的 phantom maker 与真实 queue 模型比较，量化乐观偏差；
9. 对序列缺口、延迟和坏 tick 注入扰动，结论应降级而非变得更显著；
10. 在无恢复的校准模拟器中重复全流程，控制研究管线假阳性率。

任何关键 placebo 失败，H-M 自动降为 Not-Identified；任何未来泄漏或锚被重算，整次实验作废。

## 146. 与实盘一致的实验阶梯

### X0：规则与锚审计

逐 symbol 核对官方休市、合约类型、FX、Orderbook-EWMA、Funding 和 deviation cap。输出 AnchorIntegrityCertificate 与 RuleSnapshot。

### X1：纯观察机制实验

不下单，连续捕获多个 closure episode。检验 H-A、H-R、H-D 和基础 H-M。该阶段回答“市场是否存在该现象”，不回答“我们能否成交”。

### X2：冻结样本外复制

冻结全部模型和门槛，在之后完整连续 episodes 上运行。任何人工选择行情、剔除亏损日或中途调阈值都使复制失败。

### X3：影子策略

实时产生意图但不发单，使用 live-known information；同时记录未来才可获得的研究标签到隔离 ledger。检验 online/offline feature parity。

### X4：最小 maker 探针

只为估计 queue、fill、adverse selection、cancel latency 和 self-impact。探针规模必须低于预注册市场影响预算，且不把探针 PnL用于发现假设。

### X5：资本 canary

在 H-A 至 H-E 全部通过后，用极小资本验证闭环。风险恶化立即缩容，不因短期盈利扩张。

### X6：受控扩容

只有跨 regime 的证据下界、尾部证书和实盘偏差连续满足要求才扩容。规则版本变化会自动退回 X0/X1，而不是沿用旧结论。

## 147. 本轮新增核心指标

机制指标：

- anchor integrity violation rate；
- restoring drift surface 与 simultaneous confidence band；
- \(P/I\)、\(I/A\)、\(P/A\) 三段恢复贡献；
- funding-local impulse response；
- mode-switch dynamic treatment path；
- placebo rejection ratio；
- self-reference elasticity；
- model disagreement mass。

信息指标：

- discovery gain \(DG_t\)；
- opening-direction consistency；
- permanent/transitory posterior；
- news-jump posterior calibration；
- stale-anchor loss；
- abstention precision/recall。

经济指标：

- post-fill residual convergence；
- maker fill LCB；
- adverse-fill share；
- realized-vs-predicted edge calibration；
- net edge LCB after all costs；
- forced-open unwind probability；
- capacity before Impact Mid contamination。

所有比例给 numerator、denominator、有效 episode 数和区间；所有收益给币种、notional、持有时间、成本版本和是否 observed/counterfactual。

## 148. Round 12 实施切片与新门禁

新增实施切片：

- B9：实现 AnchorDefinition、ContractTransform、RuleRegime 和四基差类型；
- B10：实现 closure EpisodeRegistry、PreregistrationManifest 和 sealed split；
- B11：实现 LocalProjection、恢复面、competing-risk 与 cluster bootstrap；
- B12：实现 latent \(V/U\) 状态空间 ensemble 和开盘 Price Discovery 标签；
- B13：实现 \(P/I+I/A\) 机制分解、mode-switch/funding/placebo suite；
- B14：实现 self-excluded LOB、maker probe 和 post-fill edge 评估；
- B15：实现 ValidationVerdict 状态机及 X0-X6 证据升级。

新门禁：

- B-G9：本币固定锚与合约计价锚、Index、Mark 不混用；
- B-G10：每个核心结论以 closure episode 为有效样本单位；
- B-G11：未来开盘标签与 online feature 物理隔离；
- B-G12：Orderbook-EWMA 自反馈和自身订单影响有显式归因；
- B-G13：未回归、未成交、止损和开盘持仓路径全部进入分母；
- B-G14：H-R、H-M、H-D、H-E、H-S 不得跨级冒充；
- B-G15：规则变化自动使旧证据过期并回退到 X0/X1。

## 149. 研究依据与采用边界

- [Binance equity TradFi Index mode update](https://www.binance.com/en/support/announcement/detail/53bfc17634f54f2f90666dbc396f5cee)：确认 2026-05-16 起股票类休盘期由 Fixed Mode 改为 Orderbook EWMA。
- [Perpetual Futures on Traditional Assets](https://www.binance.com/fr/support/faq/detail/fe7dcdf24f1943d98b368f5f9f744398)：确认 Impact Mid、EWMA、模式切换、偏离限制、A/HK 时段、Quanto 与 FX 计价。
- [Binance RCH Clearing Procedures](https://bin.bnbstatic.com/static/cms/cg08ou2ak0tn7mcplvfg/file/c8f87450c656663581984dc71672633398d1bd79e26b5981433ed01ab110c7c9.pdf)：确认 Price Index、Mark、Funding Premium、Impact Price 及 Binance 的规则调整权限。
- [Optimal Trading of Microstructure Mean Reversion](https://arxiv.org/abs/2608.00885)：支持将微观结构误差与潜在有效价格分开，并以订单簿机制解释恢复；其大 tick、外生有效价格和小交易者假设必须由本项目验证。
- [Pairs Trading with Nonlinear and Non-Gaussian State Space Models](https://arxiv.org/abs/2005.09794)：支持非线性、非高斯和异方差的潜在 spread 建模；不能把论文收益数字外推到本项目。
- [Market Simulation under Adverse Selection](https://arxiv.org/abs/2409.12721)：支持显式建模 queue、fill 与价格变化的依赖和 adverse fills。
- [Optimal Mean Reversion Trading with Transaction Costs and Stop-Loss Exit](https://arxiv.org/abs/1411.5062)：支持成本和止损改变最优交易区间；其 OU 假设只能作为 challenger。
- [The Size and Power of the Variance Ratio Test in Finite Samples](https://www.nber.org/simulations/t0066)：支持异方差稳健随机游走诊断；不作为单独的 alpha 证据。

Round 12 的最终原则是：官方收盘锚可以固定，研究假设不能固定；Binance 规则可能约束合约靠近其内生 Index，但是否进一步产生向外部收盘锚的可交易恢复力，只能由分解后的真实数据决定。实验允许且必须能够诚实地否定核心假设。


## 150. Round 13 已实施：共享假设层与 M1–M7 并行消融

本轮已将可证伪假设检验从规划层落到 Rust engine：

- 新增 `engine/src/evidence.rs`，以整数 tick/bps 计算，维护每个 symbol 的 closure episode、外部锚残差及多 horizon 前瞻样本。
- 每个公共 BookTicker/MarkPrice 事件只进入一次 `EvidenceAccumulator`；M1–M7 ledger 不各自重复采样，避免把同一行情复制成伪有效样本。
- 输出 `evidence-opportunities.jsonl`、`evidence-summary.json`，并在 `run-manifest.json` 固化 `anchorbell-evidence-v1` evidence ID、horizon 和阈值。
- 结果同时报告 (P/A)、(P/I)、(I/A) 的 bps 变化、signed improvement、样本数、改善数与 anchor integrity violations；Price Discovery、经济 edge、survival 若缺少相应标签则保持不可用/按 ledger 评估，禁止冒充 H-R 或 H-M。
- `SimulationLedgerResult` 对每个 M 变体回写同一 evidence ID，因此消融收益与假设证据可按同一公共行情流对齐。
- 消融矩阵扩展为 F1…F7 与 R7…R1；新增 M7 Evidence-Gated challenger。M7 在 M6 的动态资本基础上，对大残差与高尾部压力施加硬拒绝门，不把极端偏离自动当作均值回归 alpha。
- 修复 reduce-only post-only 方向：平多在 ask、平空在 bid，避免旧逻辑把退出单放到会立即成交的一侧。

当前证据状态：代码已完成接线，尚未因此宣称核心假设成立。必须在有真实历史行情、固定外部收盘锚、规则版本和完整成本的 run 上，读取 horizon 汇总与 ledger post-fill 结果，再按预注册门禁给出 supported / indeterminate / falsified。
## 151. Round 14 已实施：可审计研究方法层与 GNU 编译工具链

为避免把统计方法、模拟器和实盘安全边界混为一体，本轮新增 engine/src/analytics_validation.rs，仅提供可变研究层的纯函数与类型：

- AnchorDefinition、ContractTransform、RuleRegime 将锚定义、合约变换和 Binance 规则版本显式类型化；变换使用整数 i128，规则按生效区间校验，核心执行路径不接受未经验证的隐式变换。
- EpisodeRecord 与 EpisodeOutcome 以 closure episode 为单位区分 Converged、Adverse、Expired、Censored；competing_risk 将删失保留在分母，不把未闭合样本伪装成成功。
- cluster_bootstrap 以日期/episode cluster 有放回重采样，使用固定 xorshift64 seed 输出均值与 2.5%/97.5% 区间，避免逐 tick bootstrap 造成伪独立样本。
- EvidenceStateMachine 实现 X0→X6 单级递进，禁止跳级宣称“已验证”；EvidenceSummary 暴露 methodology_id 与当前 X0/X1/X2 状态，尚无经济 edge 或 survival 标签时保持分层不可用。
- LatentStateEstimate 提供整数 Kalman 风格的残差/速度/不确定性更新，作为 challenger 研究模型；它不改写不可变 AnchorSnapshot，也不替代 Binance Index/Mark。
- GNU Rust 工具链已安装到共享 runtime，cargo check --workspace --locked 通过；cargo test --workspace --locked 为 252 passed / 0 failed。rustfmt 组件亦已补装，实盘构建仍必须显式锁定工具链和 target。

仍需真实数据才能完成的部分不会被代码强行填充：Price Discovery opening label、self-excluded LOB/maker probe、post-fill edge calibration、funding/mode-switch/placebo 因果对照，以及最终 H-E/H-S 判定。这些在无对应标签时必须输出 unavailable/indeterminate，而不能由模拟器假造。

## 152. Round 15 已实施：方法计算器全部接入运行产物

本轮将上一轮列出的“尚未落地方法”全部实现为可调用、可测试、可序列化的研究计算器，并接入 simulation-batch 的 run 产物：

- classify_price_discovery：严格要求 opening reference 与 first-trade 标签；缺失时输出 Unavailable，不推导开盘价格。
- self_excluded_lob_probe：从 bid/ask 可见量中显式扣除自身挂单量，输出排除后的深度与中间价。
- evaluate_post_fill_edge：按买卖方向计算 gross/net edge、fee、funding 和 markout adverse selection。
- causal_contrast：提供 treated/control 均值差，支持 funding、mode-switch 和 placebo 样本对照。
- evaluate_survival：计算权益路径终值、最大回撤和 ruin 标志，未提供资本路径时保持不可用。
- adjudicate_verdict：只有 H-R、H-M、H-D、H-E、H-S 全部完成才输出 Supported；缺标签输出 Indeterminate，其余输出 Falsified。
- 每个 simulation-batch run 生成 validation-methods-summary.json，与共享行情、evidence summary、各策略 ledger 分离。

因此“方法”已经全部实现；真实数据不足时的 Unavailable/Indeterminate 是方法的正确结果，不是未实现，也不代表核心假设成立。

## 153. Round 16 设计冻结：M9 期限约束因果残差—联合成交收益—分布鲁棒 MPC

完整规格见 [M9_DEADLINE_CAUSAL_RESIDUAL_DRO_MPC.md](M9_DEADLINE_CAUSAL_RESIDUAL_DRO_MPC.md)。本轮只冻结设计，不声称已经实现、回测或具备实盘权限。

M9 是 M8 的严格子策略，不通过降低阈值制造交易。它新增四个可独立消融的层：统一 FairValueVersion 的因果残差、fill/markout/exit 联合结果分布、建仓时同步求解的期限退出计划、覆盖模型/队列/延迟/跳跃不确定性的分布鲁棒安全优化。

不可变核心继续由事件顺序、行情真相、AnchorContract/PriceMode、交易所规则、订单状态机、会计守恒和运行 digest 构成；M9 只能读取 M9DecisionContext 并输出 M9Decision、OrderIntent 和 ExitPlan。公平价值、状态后验、联合结果、风险半径和候选动作属于可变研究层，必须版本化且不得反写核心。

优化采用字典序：先验证数据/单位/规则和会计，再验证所有规定压力场景下的安全退出，再要求扣费净收益下置信界为正，最后才在可行集中最大化最坏情形期望对数增长。NO_ACTION 永远是合法默认动作；求解超时、模型不可识别或证书失败时不得下单。

每个 entry 必须携带最迟退出、库存轨迹、撤改单规则、资金费/开盘边界和期限残余仓位上限。shutdown 必须先停止新增风险，同时保留行情与执行通道直至撤单、退出和对账完成；不得先关闭事件源再发注定无法成交的 maker 平仓单。

正常 M9 继续严格 maker-only。若未来需要灾难主动减仓，必须作为独立、预注册、人工批准的 EmergencyExecutionPolicy 实现，不能藏在 M9 或历史模拟中。
当前证据不能作为 M9 收益先验：r7 仅 4 次成交，flat_at_end=false，queue_ahead=0、延迟为零且 build identity unknown。旧结果 capital_usdt_ticks=140,000,000,000 显示 1400.00000000 USDT；若 net_pnl_ticks 使用同一 1e8 金额刻度，5,704,463 ticks 约为 **0.05704463 USDT**，而非 5.70 USDT。会计单位合同复核前不得据此计算收益率。

M9 的晋级门固定为 X0 构建可复现、X1 市场/规则/单位/会计真相、X2 完整闭环、X3 联合分布校准、X4 对 M8 的扣费净增益下界为正、X5 尾部与退出压力合格、X6 多日多状态并通过多重选择修正。任一前置门失败，先修真实性，不优化收益。

统计单位是独立交易日/闭市会话与 closure episode，不是事件条数。消融矩阵 M9-A0 至 M9-A9 分别验证统一参考、有限期残差、Null-RW/Jump 对手、联合成交收益、队列持久、期限退出、DRO 定尺、组合相关暴露和在线降级；M9-Full 最后验证交互效果。

本轮明确拒绝固定收益承诺。200% 只能是情景分析刻度，不能作为标签、调参目标或晋级门。M9 的完成定义是：可重放、可解释、可对账、有风险证书、有外样本增益证据、失效时可安全降级。
