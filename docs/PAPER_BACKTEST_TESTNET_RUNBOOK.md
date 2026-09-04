# AnchorBell Paper、Replay 与 Testnet Runner

这份手册对应三个可执行入口：

- `anchorbell_paper`：只接 Binance 公共行情，不读凭证、不下单；当前支持
  TradFi 所需的 public BBO 流和 market mark/成交流。
- `anchorbell_backtest`：回放纸面盘保存的 JSONL，复用同一策略和 maker 成交判定。
- `anchorbell_testnet`：单标的认证运行器，默认只读；订单模式仍由独立安全开关控制。

当前实现服务于验证链路，不是收益承诺。纸面盘的 TradFi 行情由
`/public/stream` 的 `bookTicker` 与 `/market/stream` 的 mark/index、`aggTrade`
合并；成交只在“报价价位精确等于成交价，且主动方与 maker 方向相容”时发生，
尚未替代带排队位置、延迟和完整深度的生产级回测。Binance 官方历史包目前可提供
成交/聚合成交，但不能直接提供这批 TradFi 标的的历史最佳盘口，因此持续采集是
真实 maker 回测的必要数据源。

## 1. Anchor 文件

Anchor CSV 使用整数 ticks，避免浮点污染；price/quantity scale 必须按目标合约的
exchangeInfo 过滤器和行情精度配置，不能凭感觉填写：
`price = close_price_ticks / 10^price_scale`。

~~~csv
symbol,close_price_ticks,observed_at_ms,valid_until_ms
CXMTUSDT,10000,0,0
~~~

上例在 `--price-scale 2` 下表示 100.00。时间戳为 0 表示不额外限制有效期；
生产运行应填入真实收盘时间和失效时间。每个运行只允许显式的 symbol 集合。

## 1.1 直接使用 Binance 官方 indexPrice

纸面盘也支持不提供 CSV，启动时通过 Binance 公共 REST 接口读取每个选定合约的
`premiumIndex.indexPrice`。程序只在已覆盖的官方 A 股/HKEX 交易日、对应市场正式收盘
之后接受该快照为收盘锚点；盘中、周末、节假日、半日市收盘前和未知年份均拒绝新增风险。
运行期间的刷新也遵循同一收盘门禁，跨日只接受更晚的收盘快照。该流程
不读取 API key、不访问下单接口；`indexPrice` 同时会继续从实时
`markPrice@1s` 流接收并用于数据质量校验。

`PaperAnchor.close_price_ticks`、盘口、成交和 PnL 统一使用 USDT 口径。Binance
官方 TradFi 永续合约的 `indexPrice` 已按对应市场的 FX 规则归一到 USDT，因此
不能把汇率再次乘进策略锚点。为和本地收盘价核对，程序自动读取 Binance C2C
公开 USDT/CNY 和 USDT/HKD 双边报价，取中间价并计算
`index_price_local = index_price_usdt × local_currency_per_usdt`；结果写入
`--anchor-report` JSON，仅作为本币等价价和可审计证据，不改变 USDT 下单价格。

纸面盘运行期间还会启动独立的高频 FX 观测流：默认每 1,000 ms 并行读取 CNY/HKD
的 BUY 与 SELL 报价，按币种输出中间价到 `--fx-records` JSONL；默认使用
`target\paper-index-fx.jsonl`（可显式指定路径）。`--fx-max-age-ms` 默认 5,000 ms，
用于报告中的 freshness 判定；超出窗口的本地换算必须视为 stale。请求失败会指数
退避，最长 30 秒。这个 C2C 中间价是可观测的本地 USDT 参考，不声称等同于 Binance
TradFi index 内部的第三方 FX vendor；策略和交易 PnL 仍完全保持 USDT 口径。

~~~powershell
.	arget\release\anchorbell_paper.exe --index-anchors --symbols CXMTUSDT,UNITREEUSDT,CSOPSAMSUNG2LUSDT,CSOPSKHYNIX2LUSDT,GIGADEVUSDT,HK0625USDT,MINIMAXUSDT,ZHIPUUSDT,ZHONGJIUSDT --environment production --price-scale 8 --quantity-scale 8 --max-position 100000 --quantity 100000 --proxy http://127.0.0.1:7890 --duration-secs 21600 --records target\paper-index-records.jsonl --market-records target\paper-index-market.jsonl --anchor-report target\paper-index-anchor.json --fx-records target\paper-index-fx.jsonl --fx-refresh-ms 1000 --fx-max-age-ms 5000 --maker-fee-ppm 200
~~~

由于策略定义的是“底层市场收盘后的静态锚”，启动和运行时都会经过
SSE/HKEX 2026 官方交易日历门禁；盘中启动得到的 `indexPrice` 只作为行情质量输入，
不冒充收盘价。未覆盖的日历年份默认 fail-closed，需补入官方公告后才能放行。

## 2. 采集公共行情（仅选定 9 只）

从仓库根目录执行；当前远端网络需要 HTTP CONNECT 代理时，显式传入代理：

~~~powershell
cargo run -p static-anchor-engine --bin anchorbell_paper --locked -- --index-anchors --symbols CXMTUSDT,UNITREEUSDT,CSOPSAMSUNG2LUSDT,CSOPSKHYNIX2LUSDT,GIGADEVUSDT,HK0625USDT,MINIMAXUSDT,ZHIPUUSDT,ZHONGJIUSDT --environment production --price-scale 8 --quantity-scale 8 --max-position 100000 --quantity 100000 --proxy http://127.0.0.1:7890 --duration-secs 300 --records runs\paper-records.jsonl --market-records runs\market.jsonl --anchor-report runs\paper-anchor.json
~~~

这 9 只标的分别从 `/public/stream` 订阅 `bookTicker`，从
`/market/stream` 订阅 `markPrice@1s` 和 `aggTrade`。策略决策写入
`paper-records.jsonl`，规范化行情和本地 receipt timestamp 写入
`market.jsonl`。纸面入口会拒绝执行白名单之外的 symbol；写盘是有界异步旁路，
队列拥塞会计数，不改变策略回调。

## 3. 回放同一份行情

回测输入可以是上述带 receipt timestamp 的 envelope，也可以是单行一个原始
Binance WebSocket JSON。回放严格检查时间顺序，遇到乱序会失败，不会静默排序：

~~~powershell
cargo run -p static-anchor-engine --bin anchorbell_backtest --locked -- --input runs\market.jsonl --anchors data\anchors.csv --price-scale 8 --quantity-scale 8 --entry-threshold-bps 0 --max-position 100000 --maker-fee-ppm 200 --quantity 100000 --records runs\replay-records.jsonl
~~~

输出包含事件数、订单数、成交数、成交数量、市场持仓 PnL、策略执行 PnL、
资金费、maker 手续费、总收益和净 PnL ticks、未实现估值完整性、峰值绝对仓位、
当前仓位、挂单数和 `flat_at_end`；纸盘 metrics 还保存 900 个历史快照及九标的分项归因，
并带输入 SHA-256。窗口结束时只撤销仍挂着的被动报价，不会凭空生成平仓成交；
因此带持仓的窗口只能看作未完成窗口。对完整交易窗口可追加
`--require-flat-at-end`，若仍有持仓或挂单则命令失败。回测输出是模型结果，
不能当作 Testnet 或 Production 成交证据。

纸面/回放退出使用与执行适配器相同的被动 maker 决策：多仓只在卖一挂 SELL，
空仓只在买一挂 BUY；仅收到价格精确匹配且主动方相容的聚合成交后才改变仓位。
提交、取消请求和取消确认都不是成交。若没有下载的交易所过滤器，模拟器明确采用
有限的模拟假设：price tick=1、quantity tick=1、minimum quantity=1、minimum
notional=1 quote tick、maximum quantity=该标的配置的 `max_position`；这些记录为
simulation，绝不表示已获取 exchangeInfo。显式提供的过滤器保留其观测时间并会过期。
回放时钟也会驱动退出评估；盘口 receipt/event 时间独立于 mark，过期、未来、零价差
或倒挂盘口不创建退出单。若配置了取消延迟，撤单在确认前仍可能按原有 maker 规则成交，
确认后才允许替换。硬截止后不再补单，风险增加的本地挂单会进入撤单路径，安全的
reduce-only 挂单会继续跟踪并将剩余仓位记录为 residual exposure。

EOF 的 `cancel_all` 是本地模拟器 teardown，不是交换所的取消确认：它只清除纸面
working order 并写 `order_teardown` 记录，不伪造成交或正常撤单确认时间。

## 4. Testnet 认证只读运行

先在当前 PowerShell 会话注入 Testnet 凭证；也可以由 Dashboard 保存到当前
Windows 用户的 Credential Manager，runner 会按 `testnet` 环境读取对应条目。

~~~powershell
$env:ANCHORBELL_BINANCE_ENV = "testnet"
$env:ANCHORBELL_BINANCE_API_KEY = "<testnet-key>"
$env:ANCHORBELL_BINANCE_API_SECRET = "<testnet-secret>"
$env:ANCHORBELL_HTTP_PROXY = "http://127.0.0.1:7890"

cargo run -p static-anchor-engine --bin anchorbell_testnet --locked -- --symbol BTCUSDT --anchor-ticks 10000000000000 --price-scale 8 --quantity-scale 8 --duration-secs 60 --proxy http://127.0.0.1:7890
~~~

启动前会校验环境、服务器时间、当前挂单和仓位；运行中轮询订单/仓位并在
状态不确定时 halt。默认只打印 maker proposal，不调用下单接口。当前
Binance Testnet 未必提供 AnchorBell 目标股票永续；BTCUSDT 只能验证通用
认证、签名、订单生命周期和恢复契约，不能代表目标标的。

## 5. 受控 Testnet 订单

只有在只读运行、账户状态和取消路径都有脱敏证据后，才可显式打开 Testnet
订单权限。下单仍固定为 `LIMIT + GTX`，启动要求空仓，单次只维护一个本地
AnchorBell 挂单；未知远端状态会停止自动增加风险。

~~~powershell
$env:ANCHORBELL_ENABLE_ORDER_SUBMISSION = "1"
cargo run -p static-anchor-engine --bin anchorbell_testnet --locked -- --symbol BTCUSDT --anchor-ticks 10000000000000 --price-scale 8 --quantity-scale 8 --duration-secs 60 --send-orders
~~~

`--send-orders` 是真实 Testnet 请求，不是纸面盘。Production 还需要独立的
Production 开关、Production 凭证和精确确认字符串；本手册不建议直接开启
Production，也不把 Testnet 结果等同于可盈利实盘。

## 6. 停止与证据

正常到时或 Ctrl-C 会先撤掉本地 working order，再核对仓位和挂单。若发生
未知订单/账户/行情状态，程序会 halt 并保留人工处理所需状态，不会盲目撤单
或继续加仓。保存 commit SHA、环境、symbol、配置摘要、UTC 时间、订单状态
序列和最终仓位；不要保存 API secret、完整签名 payload 或未经脱敏的账户响应。

官方接口边界见 Binance [USDⓈ-M Futures General Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)、
[New Order](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order)
和 [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)。
