# AnchorBell：Binance 股票永续锚定做市引擎

[English](README.md) · **简体中文**

[![Rust](https://img.shields.io/badge/Rust-2021-orange?logo=rust)](https://www.rust-lang.org/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-green.svg)](LICENSE)
[![Exchange](https://img.shields.io/badge/exchange-Binance-F0B90B)](https://www.binance.com/)
[![Execution](https://img.shields.io/badge/execution-maker--first-blue)](docs/TESTNET_RUNBOOK.md)

<p align="center">
  <strong>研究收盘锚点，报价价格偏离，风险截止前降低残余风险。</strong><br>
  Rust-first、maker-first 的 Binance 股票相关永续合约研究与受控 Testnet/Production 执行引擎，带有严格 reduce-only 的紧急 taker 路径。
</p>

AnchorBell 是一个 Rust-first、maker-first 的 Binance 股票相关永续合约工业量化服务，带有仅用于降低残余风险的自适应 reduce-only 紧急 taker 路径，覆盖
实时行情、隔离模拟、历史回放、受控 Testnet/Production 执行、风险控制、订单生命周期、恢复和可观测性。

它不是套利获利承诺，也不是投资建议。所有结果都必须明确数据、延迟、成交、手续费和
风险假设。

## 核心思想

底层股票市场休市后，永续合约可能偏离最近可靠的股票市场收盘价。AnchorBell 将收盘价
建模为具有明确有效期的静态锚点，评估偏离，优先只挂 post-only 被动订单，并在底层市场重新开盘或资金费风险截止前尝试被动减仓。未成交的残余仓位会明确报告，不会被视为已经平仓；只有在配置注册、reduce-only、截止期可行性、成本与安全门禁同时满足时，才允许使用紧急 taker 降低残余风险。港股发行人的 ADR/ADS 只有在收盘后仍提供有效价格发现时才排除；
低流动性、过期或无有效报价的 OTC 无担保 ADR 会被记录，但不会参与锚点计算。

## 配置与架构治理

交易所规则、手续费、标的清单、执行策略和实验参数必须来自 Binance 元数据、用户输入或版本化 JSON/TOML 配置；策略与执行代码不得暗藏业务默认值。每次实验必须记录配置版本、来源、哈希、数据窗口、费用模型、成交假设和风险门禁结果，才能与其他方法公平比较。

## 系统边界

- `market`：Binance 行情解析、订阅与 JSONL 录制
- `strategy`：锚点、交易时段、报价和库存策略
- `execution`：订单意图、生命周期、风控、凭证和传输契约
- `replay`：严格按时间排序的历史事件回放
- `backtest`：可替换的 maker 成交假设与回测报告
- `m8`：资金费率感知的鲁棒锚控制纯策略数学层

策略不会自行读取凭证、建立网络连接或直接修改交易所状态；网络适配器在边界之外注入。

## 测试网与历史回测

项目已经包含 Testnet 与 Production 的显式端点配置、签名订单传输契约、行情 JSONL
录制、事件回放和保守的盘口成交模型。Production 默认不启用。

K 线回测不足以评估 maker 策略。严肃回测至少应记录 bookTicker、mark price、收盘锚点、
本地接收时间、延迟、排队假设、撤单时机、手续费和资金费率。

详见[模拟/回放/Testnet 运行手册](docs/SIMULATION_BACKTEST_TESTNET_RUNBOOK.md)、[M8 资金费率鲁棒控制设计](docs/M8_FUNDING_AWARE_ROBUST_CONTROL.md)、[测试网与历史回放](docs/TESTNET_AND_BACKTEST.md)、[Futures 测试网手册](docs/TESTNET_RUNBOOK.md)、[双环境手册](docs/DUAL_ENVIRONMENT_RUNBOOK.md)和[Spot Demo 现货模拟盘手册](docs/SPOT_DEMO_RUNBOOK.md)。

## 快速开始

双击仓库根目录的 `Start-AnchorBell-Dashboard.cmd` 即可启动本地控制台，不需要手动输入
cargo 命令。控制台只监听 127.0.0.1，不对局域网开放。

```powershell
git clone https://github.com/Xalzeroph/AnchorBell.git
cd AnchorBell
cargo test --workspace --locked
cargo run -p anchorbell-engine
```

默认使用 Testnet，并通过环境变量提供凭证。通用只读 smoke 不会下单：

```powershell
$env:ANCHORBELL_BINANCE_ENV = "testnet"
$env:ANCHORBELL_BINANCE_API_KEY = "<testnet-key>"
$env:ANCHORBELL_BINANCE_API_SECRET = "<testnet-secret>"
cargo run -p anchorbell-engine --bin binance_account_smoke --locked
cargo run -p anchorbell-engine --bin binance_open_orders_smoke --locked
```

Production 只读和真实订单的独立开关、凭证变量及确认要求见[双环境手册](docs/DUAL_ENVIRONMENT_RUNBOOK.md)。

## 开发与安全

修改应保持边界清晰，附带针对性测试和文档。不得提交 API 密钥、私钥、账户信息或
认证后的原始载荷。生产下单必须经过显式安全门禁。

- [架构治理契约](docs/ARCHITECTURE_GOVERNANCE.md)
- [贡献指南](CONTRIBUTING.md)
- [安全策略](SECURITY.md)
- [Apache License 2.0](LICENSE)
- [行为准则](CODE_OF_CONDUCT.md)
