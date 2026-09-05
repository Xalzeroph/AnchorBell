# AnchorBell Testnet Runbook

本手册用于第一次真实 Binance Futures Testnet 验证。它不授权 Production，也不代表可以无人值守运行。

## 当前边界

- 只允许 Binance USDⓈ-M Futures Testnet。
- 生产环境默认关闭；任何环境错配都会在网络连接前 fail-closed。
- 只允许 `LIMIT + GTX` 被动订单；禁止市价单和主动成交。
- 测试凭证只能通过进程环境或本地控制台注入，不得写入普通文件、浏览器存储、日志、回放数据或提交。
- 控制台的“保存到本机凭证库”使用远端 Windows 当前用户的 Credential Manager；Testnet 与 Production 使用不同条目，API Secret 不会出现在仓库或前端存储中。
- “应用当前会话”只更新内存会话；输入框留空时会加载当前环境的已保存凭证。“清除当前会话”不会删除凭证库；需要删除时使用“删除本机保存凭证”。
- 未提供凭证且凭证库没有对应条目时，程序必须停在缺少凭证状态。

## Latest public-market smoke evidence

2026-09-01 在远端 Windows 上执行 `testnet_market_smoke`，通过显式注入的本机 HTTP CONNECT 代理 `http://127.0.0.1:7890` 建立 Rust WebSocket 连接，在 12 秒窗口内收到并解析 102 个 BTCUSDT `bookTicker`/`markPrice` 事件。

- Rust 连接层已支持可选代理、IPv4 优先地址连接、TLS CryptoProvider 初始化、连接总时限和 fail-closed 错误返回；
- 事件中包含盘口价格/数量、mark price、index price、next funding time 和 funding rate；
- 不设置代理时仍会在连接门禁内停止，不会降级为隐藏的其他执行路径；
- 此证据只闭合公共行情链路，未证明认证订单、成交、撤单、恢复或真实资金安全。

P4 的认证阶段仍需用户注入专用 Testnet 凭证后逐阶段验证，不能发送任何订单作为“烟测”替代。Production 使用独立凭证和独立门禁，详见双环境手册。

## Latest TradFi metadata evidence

2026-09-02 在远端 Windows 通过显式 HTTP CONNECT 代理执行公开元数据 smoke，未使用 API 凭证且未发送订单：

- Production：9/9 个 AnchorBell 执行标的通过 `exchangeInfo`、双边 `bookTicker`、`premiumIndex`、TRADIFI_PERPETUAL、正数市场值、资金费率格式和未来资金费率时间门禁；
- Testnet：9/9 个标的不可用，其中部分不在 `exchangeInfo`，其余为 `PENDING_TRADING`；这证明 Testnet 当前不能为这组股票永续提供真实合约仿真覆盖；
- Testnet 仍可用于通用连接、签名、生命周期和恢复契约验证，但不能把 BTCUSDT 等非目标标的的结果等同于股票永续证据。

## Read-only authenticated smoke

凭证注入后先执行：

```powershell
cargo run -p static-anchor-engine --bin binance_account_smoke --locked
```

该入口只发送签名的 `account.status` 查询，不包含 `order.place`、撤单或任何改变账户状态的请求。随后可按 symbol 执行当前挂单只读查询：

```powershell
$env:ANCHORBELL_SYMBOL = "BTCUSDT"
cargo run -p static-anchor-engine --bin binance_open_orders_smoke --locked
```

该入口调用 signed REST `GET /fapi/v1/openOrders`，只输出挂单数量，不输出响应原文或任何凭证。只有账户状态与挂单查询都成功并保存脱敏证据后，才允许进入单笔 maker 订单阶段。

## TradFi-Perps 协议确认

Binance 为股票相关永续提供独立的账户级协议接口：`POST /fapi/v1/stock/contract`。
AnchorBell 通过控制台的“TradFi 协议”按钮调用该签名接口；它不提交订单，但会改变账户协议确认状态，
因此不会在启动时自动执行。只有用户明确点击后，才会向当前选定环境发送该请求。

## 运行前检查

1. 使用专用 Testnet API key，只开启读取与交易所需的最小权限。
2. 在测试配置中填写明确的 symbol allowlist、tick/step、最小名义金额、仓位上限和 session 窗口。
3. 仅在远端 PowerShell 进程中设置凭证：

```powershell
$env:ANCHORBELL_BINANCE_API_KEY = "<testnet-key>"
$env:ANCHORBELL_BINANCE_API_SECRET = "<testnet-secret>"
```

不得把上述值放入 `.env`、源码、命令历史、issue、日志或录屏。

## 分阶段验证

### A. 无凭证公共行情烟测

先验证 Testnet public market WebSocket、bookTicker/markPrice 解码、ping/pong、帧大小限制和重连。此阶段不得发送订单。

### B. 认证但零风险请求

验证凭证加载、签名 canonicalization、账户/订单查询和时间窗口错误处理。只允许读取类请求；任何未知响应都不能生成成交事件。

### C. 单笔 maker 订单

使用极小数量和明确的 allowlisted symbol：提交一笔远离盘口的 `LIMIT + GTX`，立即执行 cancel，核对 request id、exchange order id、本地生命周期和最终 remote status。若被判定会立即成交，必须停止测试。

### D. 受控成交与恢复

只有 A-C 全部有证据后，才可分别验证 partial fill、full fill、断线恢复、重启 reconciliation、stale-data halt 和 session flatten。每个场景单独运行，发生未知状态立即停止。

Maker-only 的退出只能作为风险降低目标，不能表述成成交保证。多仓退出挂卖一、
空仓退出挂买一，并保持 post-only/reduce-only；到达股票开盘或资金费硬截止仍未成交时，
必须记录 residual exposure，不能伪造成平仓成功，也不能自动改用 taker/市价单。
只读验证禁止包括撤单在内的全部交易写操作。

## 证据要求

每个场景保存：commit SHA、配置摘要（脱敏）、symbol、UTC 时间、请求 id、exchange order id、订单状态序列、风险状态、重连次数和最终仓位。不得保存 API secret 或完整认证 payload。

## 明确禁止

- 不得把 Testnet 通过等同于 Production 可用。
- 不得在没有人工复核的情况下开启 Production。
- 不得用回测成交结果证明真实流动性或收益。
- 不得因网络、订单或账户状态不确定而自动增加风险。
