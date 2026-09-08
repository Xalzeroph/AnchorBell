# AnchorBell Dual Environment Runbook

本手册说明 AnchorBell 如何在 Binance Futures Testnet 与 Production 之间切换。
默认永远是 Testnet；Production 必须显式打开。所有命令都只在当前 PowerShell
进程中读取凭证，不把凭证写入仓库、配置文件、日志或回放文件。

## 运行模式

| 模式 | 环境变量 | 凭证变量 | 订单权限 |
| --- | --- | --- | --- |
| Testnet | ANCHORBELL_BINANCE_ENV=testnet 或不设置 | ANCHORBELL_BINANCE_API_KEY/SECRET | 默认关闭 |
| Production 只读 | ANCHORBELL_BINANCE_ENV=production + ANCHORBELL_ENABLE_PRODUCTION=1 | ANCHORBELL_BINANCE_LIVE_API_KEY/SECRET | 关闭 |
| Production 订单 | 在只读配置上再打开订单开关并确认 | 同上 | 独立显式开启 |

程序不会把 Testnet 凭证用于 Production，也不会把 Production 凭证用于 Testnet。
缺少凭证、环境错配或确认不完整时，程序在网络连接前停止。推荐通过根目录的
Start-AnchorBell-Dashboard.cmd 打开本地控制台完成配置；控制台不会把凭证写入浏览器
存储或项目文件。

## Testnet

```powershell
$env:ANCHORBELL_BINANCE_ENV = "testnet"
$env:ANCHORBELL_BINANCE_API_KEY = "<testnet-key>"
$env:ANCHORBELL_BINANCE_API_SECRET = "<testnet-secret>"

cargo run -p anchorbell-engine --bin binance_account_smoke --locked
cargo run -p anchorbell-engine --bin binance_open_orders_smoke --locked
```

两个 smoke 都是签名只读查询：分别调用账户状态和当前挂单查询，不会下单、
撤单或改变账户状态。TradFi 股票永续还需要用户主动点击控制台的“TradFi 协议”检查，
该操作调用签名的 `POST /fapi/v1/stock/contract`，不提交订单，但会确认账户协议状态。
若只测试公共行情，不需要任何凭证：

```powershell
cargo run -p anchorbell-engine --bin testnet_market_smoke --locked
```

## Production 只读验证

只读验证需要 Production 环境开关和 Production 专用凭证，但不需要订单开关：

```powershell
$env:ANCHORBELL_BINANCE_ENV = "production"
$env:ANCHORBELL_ENABLE_PRODUCTION = "1"
$env:ANCHORBELL_BINANCE_LIVE_API_KEY = "<production-key>"
$env:ANCHORBELL_BINANCE_LIVE_API_SECRET = "<production-secret>"

cargo run -p anchorbell-engine --bin binance_account_smoke --locked
cargo run -p anchorbell-engine --bin binance_open_orders_smoke --locked
```

这两个入口仍然只读，且不会因为设置了 Production 环境就自动下单。
建议先使用只读 API key 验证网络、签名、时间窗口和账户权限。

## Production 订单权限

真实订单需要三个条件同时满足：

```powershell
$env:ANCHORBELL_BINANCE_ENV = "production"
$env:ANCHORBELL_ENABLE_PRODUCTION = "1"
$env:ANCHORBELL_ENABLE_ORDER_SUBMISSION = "1"
$env:ANCHORBELL_LIVE_TRADING_CONFIRMATION = "I_UNDERSTAND_REAL_FUNDS_RISK"
```

还必须提供 ANCHORBELL_BINANCE_LIVE_API_KEY 与
ANCHORBELL_BINANCE_LIVE_API_SECRET。订单传输层会再次检查 policy；没有订单
权限时，order.place 在发送到 Binance 之前被拒绝。现有 generic smoke
入口永远不发送订单，因此不能把 smoke 当作真实下单命令。

`anchorbell_live` 还要求命令行显式提供 `--send-orders`。环境订单开关和该参数
必须同时为真；否则统一执行许可边界禁止所有交易写操作，包括开仓、reduce-only
退出单、改单前撤单和停止清理撤单。只读模式仍可进行账户/订单查询、行情订阅和
listen-key 管理，但不会借“风险清理”绕过只读限制。

当前 live runner 固定为项目的 9 个 TradFi 标的，启动时逐标的要求空仓且无任何
现有挂单。`anchorbell-` 前缀本身不构成订单所有权证明；发现启动前挂单会停止，
不会批量撤销账户中其他策略的订单。运行中只撤销本进程内已登记的精确 client order
id。事件队列丢失、订单结果未知、成交数量回退/超量、仓位快照早于最新成交等情况
都会 fail-closed。

退出检查由行情/账户事件和独立 250 ms 定时器共同驱动。它取股票开盘风险边界与
资金费边界中较早者，使用每 60 秒刷新的 exchangeInfo 规则（静态规则 120 秒过期）
及最长 5 秒的盘口/mark 快照，只在卖一被动减多仓、买一被动减空仓，并设置
`reduceOnly=true`。到达硬截止仍未成交时只报告 residual exposure；不会制造成交、
宣称已平仓或自动切换 taker/市价单。配置时长结束、Ctrl-C 和错误路径共享停止清理，
但只能撤销许可范围内且所有权明确的订单；最终仓位查询失败会明确标为 unknown。

这些保护仍不能替代真实 Testnet 生命周期证据、Production 人工复核和外部监控。
当前 Testnet 缺少这 9 个可交易 TradFi 合约，因此本地离线测试不能证明交易所会接受
这些订单，也不能证明按时成交或盈利。

## 凭证与清理

```powershell
Remove-Item Env:ANCHORBELL_BINANCE_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:ANCHORBELL_BINANCE_API_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:ANCHORBELL_BINANCE_LIVE_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:ANCHORBELL_BINANCE_LIVE_API_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:ANCHORBELL_LIVE_TRADING_CONFIRMATION -ErrorAction SilentlyContinue
```

不要把真实 key/secret 放入命令历史、截图、CI 日志、issue、提交或聊天。建议
Production key 关闭提现权限，并只授予当前验证所需的最小权限。

## 证据要求

保存 commit SHA、环境名称、脱敏配置摘要、UTC 时间、symbol、请求 id、HTTP/
WebSocket 状态、错误分类和最终仓位；不得保存 secret、完整认证 payload 或原始
账户响应。Testnet 通过不等于 Production 的流动性、延迟、成交或收益已被证明。

Binance Futures 的 Testnet 与 Production 使用不同的 REST/WebSocket 基地址；
以官方文档为准：
[USDS-M Futures General Information](https://developers.binance.com/en/docs/derivatives/usds-margined-futures/general-info)
和 [WebSocket API General Information](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-api-general-info)。
