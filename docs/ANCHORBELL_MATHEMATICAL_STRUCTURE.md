# AnchorBell 统一数学结构

## 1. 核心对象

AnchorBell 不是价格预测器，而是受证据约束的行动选择器。它只在外部股票市场闭市期间，给定合法外部收盘锚，判断是否以 Binance 永续合约的 maker 订单获取可兑现的收敛收益。系统只运行 simulation。

基础集合为标的 \(\mathcal I\)、交易场所 \(\mathcal V\)、事件时间 \(\mathcal T\)、价格单位 \(\mathcal Q_p\)、数量单位 \(\mathcal Q_q\)、行动集合 \(\mathcal A\)、事件集合 \(\mathcal E\) 和证据摘要集合 \(\mathcal D\)。价格和数量使用整数最小单位，收益使用有符号 pico-bps，展示层才转成 USDT 或人民币。

## 2. 锚与 episode

合法锚不是 Binance 价格，而是外部市场已完成收盘事实：

\[
\alpha=(i,v,d,\tau_c,p_c,\sigma_\alpha).
\]

合法性要求：

\[
p_c>0,\quad \tau_c>0,\quad t\ge\tau_c,\quad source(\alpha)\ne\varnothing,
\quad closeCompleted(\alpha)=true.
\]

停牌、节假日、半日市、企业行为、换约、币种或汇率变换存在歧义时，锚为 Unknown。不能用 Binance 价格反推合法收盘锚。

交易 episode 为 \(e=(\alpha,w,f,\iota_e)\)。时间边界为：

\[
\tau_c<\tau_{\mathrm{entry}}
\le\tau_{\mathrm{external\ open}}
\le\tau_{\mathrm{flatten}}.
\]

资金费会提前改变退出边界：

\[
\tau_{\mathrm{exit}}
=\min(\tau_{\mathrm{external\ open}},\tau_{\mathrm{funding}}).
\]
新增风险只在闭市且未越过 entry deadline 时开放；之后只能减仓；硬截止后只能应急退出或报告 residual exposure。

## 3. 信息过滤

时刻 \(t\) 的状态为：

\[
X_t=(E_t,M_t,C_t,\Pi_t,R_t,K_t,L_t),
\]

分别表示 episode/锚、市场、合约、账户、组合资本、校准和账本。每个事实都具有：

\[
x=(value,eventTime,observedTime,source,digest,freshnessBound).
\]

事实可用当且仅当：

\[eventTime(x)\le t,\quad observedTime(x)\le t,\quad
t-observedTime(x)\le\Delta_x,
\]

且 source、digest 非空。信息采用证据偏序：新状态必须保留事实、来源、事件时间和失效边界；数值相同不代表证据相同。Unknown 不等于零。

## 4. 市场与账户

市场状态：

\[
M_t=(B_t,I_t,P_t,T_t,\sigma_M),
\]

其中 \(B_t=(bid,ask,q_b,q_a,sequence)\)，\(I_t\) 是指数价，\(P_t\) 是标记价。必须有：

\[
0<bid<ask,\quad q_b>0,\quad q_a>0,\quad sequence>0.
\]
锁盘或交叉盘口不属于 maker 可行域；盘口、指数、标记价和服务器时间分别检查新鲜度。

账户状态：

\[
\Pi_t=(mode,\pi,\pi^L,\pi^S,\bar\pi,reconciled,\sigma_\Pi).
\]

One-way 使用有符号净持仓 \(\pi\)，Hedge 使用非负 long/short 两腿：

\[
mode=OneWay\Rightarrow\pi^L=\pi^S=0,
\quad
mode=Hedge\Rightarrow\pi=0.
\]

Hedge 两腿同时存在时，即使 \(\pi^L-\pi^S=0\)，风险 \(\pi^L+\pi^S\) 仍不为零；单订单决策必须返回 residual exposure。

组合资本状态单独建模为：

\[
R_t=(E_t,A_t,M_t,Q_t,S_t,U_t,\sigma_R),
\]

其中 \(E_t\) 为权益，\(A_t\) 为可用保证金，\(M_t\) 为维持保证金，\(Q_t\) 为已保留保证金，\(S_t\) 为压力预算，\(U_t\) 为已使用压力预算。其公理为：

\[
0\le M_t,Q_t\le E_t,\quad 0\le U_t\le S_t,
\]

\[
A_t\ge m(o)+M_t,\quad U_t+s(o)\le S_t
\]

才允许新增风险订单 \(o\)；任一量缺失、过期、溢出、来源摘要为空，均不允许新增风险。这里 \(m(o)\) 与 \(s(o)\) 必须由账户/合约风控事实计算，策略不得臆造杠杆、保证金或压力成本。

## 5. Binance 可行域
订单 \(o\) 合法，当且仅当合约 Trading 且新鲜，价格和数量分别满足：

\[
p\in[p_{\min},p_{\max}],\quad p\bmod tickSize=0,
\]

\[
q\in[q_{\min},q_{\max}],\quad q\bmod stepSize=0.
\]

同时满足 minNotional，若交易所提供则满足 maxNotional；并且 position mode、positionSide、reduceOnly、TIF、STP、订单容量、rate limit、保证金和合约身份全部一致。

普通新增风险的可行域：

\[
\mathcal A_t^{maker}
=\{o:TIF(o)=GTX,\ cross(o,B_t)=0\}.
\]

One-way 减仓使用 reduceOnly；Hedge 不发送 reduceOnly，而使用 positionSide 与反向方向证明减仓。硬截止 IOC 是独立 emergency 集合，不是 maker 的隐式降级。
## 6. 成交因果与收益

队列前数量 \(q_a\)、订单量 \(q_o\)、激活及延迟后的成交穿透 \(q_t\) 给出：

\[
q_{\mathrm{causal}}
=\min(q_o,\max(0,q_t-q_a)),
\quad
p_{\mathrm{fill}}
=\left\lfloor10000q_{\mathrm{causal}}/q_o\right\rfloor.
\]

本地意图不能创造成交；交易所权威成交必须满足：

\[
q_{\mathrm{exchange}}\le q_{\mathrm{causal}}.
\]

路径 \(c\) 的毛收敛价值：
\[
G(c)=\left\lfloor
\frac{s(p_{\mathrm{exit}}-p_{\mathrm{entry}})
q_{\mathrm{exit}}10^{12}}
{p_{\mathrm{anchor}}q_{\mathrm{requested}}}
\right\rfloor.
\]

净值：

\[
N(c)=G(c)-F_{\mathrm{entry}}-F_{\mathrm{exit}}
-C_{\mathrm{exit}}-C_{\mathrm{funding}}-C_{\mathrm{deadline}}.
\]

场景权重为正且总和 10000；等待也是行动，必须有自己的分布。不确定性预算：

\[
U=U_{\mathrm{anchor}}+U_{\mathrm{execution}}+U_{\mathrm{timing}}+U_{\mathrm{model}},
\]

保守下界：

\[
L(D)=\left\lfloor\sum_iw_iN_i/10000\right\rfloor-U.
\]

## 7. 决策与自进化

校准方向 \(s\) 的执行值：

\[
V_{\mathrm{exec}}(s)
=\left\lfloor L(D_s)p_s/10000\right\rfloor+m_s.
\]

决策：
\[
a_t^*\in\arg\max_{a\in
\{Wait\}\cup A_t^{maker}\cup A_t^{reduce}\cup A_t^{emergency}}
V_t(a).
\]

新增风险还必须满足：

\[
V_t(a_t^*)>V_t(Wait)
\land V_t(a_t^*)>V_t(a_{\mathrm{opposite}}).
\]

相等且队列证据无法打破时返回 AmbiguousDirection。

校准状态为 \(K_t=(O_t,\theta_t,\rho_t,\delta_t)\)，观测通过：

\[
O_{t+1}=AppendValidate(O_t,o_t),
\quad Replay(O_t)=O_t.\]

ColdStart 和 Validating 禁止新增风险；隐藏 bootstrap、事件时间倒退、非法数量、成交超过委托量和重放不一致都拒绝。

策略计划为 \(P=(V,N,I)\)，候选只能在锚、闭市、因果成交、Binance 合法性、maker 新增风险、对账、组合资本、样本充分性、回撤和 sealed set 全部满足时比较。演化只能改变估计器，不能删除核心不变量。

## 8. 订单生命周期与撤单竞态

订单状态集合为：

\[
S=\{Intent,Submitted,Accepted,PartiallyFilled,Filled,
CancelPending,Canceled,Rejected,Unknown\}.
\]

生命周期不是总序，而是允许集合 \(T\subseteq S\times S\)。Filled、Canceled、Rejected 是互斥终态；CancelPending 之后允许交易所权威的部分成交或成交，因为撤单请求与成交回报存在竞态。成交、撤销确认和拒绝不能由本地策略自证；本地只能产生意图、提交和撤单请求。未知状态只能由交易所或可信回放恢复。每条事件须满足单调时间、唯一事件号、累计数量单调、成交不超过因果队列吞吐。

## 9. 仍需继续消除的缺口

当前组合层已具备组合快照与逐订单 ReservationBook：每个在途订单的保证金/压力占用都可重放、释放和结算；仍需把它与真实仿真运行时的下单确认、撤单确认和成交结算做原子事务绑定。保证金、杠杆、维持保证金、强平价格和手续费仍需按 Binance 合约事实版本化。多源锚冲突需输出不可判定而不是平均。跨源乱序、重复、修订和回补事件需进入统一重放模型；连续仿真运行时也尚未接入当前新核心。

核心链条：

\[合法锚\rightarrow闭市episode\rightarrow\mathcal F_t
\rightarrow\mathcal A_t\rightarrow V_t\rightarrow保守最优行动.
\]

任一环节不可证明，返回等待、减仓或 residual exposure。自进化不能篡改过去、创造成交或把未知解释为安全。