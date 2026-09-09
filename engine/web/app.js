const $ = (id) => document.getElementById(id);
const modeMeta = {
  live: { label: "实盘", eyebrow: "LIVE / EXECUTION", title: "实盘控制台", subtitle: "连接账户与市场实时数据，所有订单动作均经过独立安全门禁。", start: "启动实盘", hint: "启动前请先完成环境与安全设置。默认只读，不提交真实订单。" },
  simulation: { label: "模拟执行", eyebrow: "SIMULATION / EXECUTION", title: "模拟执行控制台", subtitle: "使用标准化公共行情，在隔离执行环境中验证策略、成交与风险。", start: "启动模拟执行", hint: "模拟执行不读取真实订单权限，不调用生产订单接口。" },
  backtest: { label: "回测", eyebrow: "HISTORICAL REPLAY", title: "回测分析", subtitle: "对历史行情进行独立回放，结果、日志和记录文件单独保存。", start: "启动回测", hint: "回测是一次性任务，完成后可在结果区查看 JSON 报告。" }
};
let currentMode = "live";
let runtimes = {};
let logTimer = null;

function escapeHtml(value) { return String(value == null ? "" : value).replace(/[&<>"']/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c])); }
function signed(value) { if (value == null || Number.isNaN(Number(value))) return "—"; const n = Number(value); return (n > 0 ? "+" : "") + n.toLocaleString(); }
function number(value) { return value == null || Number.isNaN(Number(value)) ? "—" : Number(value).toLocaleString(); }
function setMessage(message, kind) { const node = $("sessionMessage"); node.textContent = message; node.className = "inline-message " + (kind || ""); }
function notify(message, kind) { const node = $("modeLogs"); const line = "[" + new Date().toLocaleTimeString() + "] " + message; node.textContent = node.textContent === "暂无运行日志" ? line : line + "\n" + node.textContent; node.classList.toggle("error", kind === "error"); }

async function api(path, options) {
  const response = await fetch(path, Object.assign({ cache: "no-store" }, options || {}, { headers: Object.assign({ "Content-Type": "application/json" }, (options && options.headers) || {}) }));
  let data = {};
  try { data = await response.json(); } catch (_) { throw new Error("服务返回了无效响应"); }
  if (!response.ok || data.ok === false) throw new Error(data.message || "请求失败");
  return data;
}
function switchMode(mode) {
  currentMode = mode;
  document.querySelectorAll(".mode-link").forEach((b) => b.classList.toggle("active", b.dataset.mode === mode));
  document.querySelectorAll(".mode-page").forEach((p) => p.classList.toggle("active", p.id === mode + "Page"));
  const meta = modeMeta[mode];
  $("modeEyebrow").textContent = meta.eyebrow; $("modeTitle").textContent = meta.title; $("modeSubtitle").textContent = meta.subtitle;
  $("controlTitle").textContent = meta.label + "进程"; $("controlHint").textContent = meta.hint; $("startMode").textContent = meta.start;
  updateRuntimeCards(); refreshModeData();
}
function runtimeFor(mode) { return runtimes[mode || currentMode] || { mode: mode || currentMode, status: "stopped" }; }
function formatAge(ms) { const s = Math.max(0, Math.floor(ms / 1000)); const h = Math.floor(s / 3600); const m = Math.floor((s % 3600) / 60); return h ? h + "h " + m + "m" : m + "m " + (s % 60) + "s"; }
function updateRuntimeCards() {
  const r = runtimeFor(); const running = r.status === "running"; const meta = modeMeta[currentMode];
  const sourceHint = r.source === "systemd_batch" ? "systemd 批处理 · 只读观测" : "独立进程运行中";
  $("runtimeModeLabel").textContent = meta.label; $("runtimeModeHint").textContent = running ? sourceHint : (r.last_message || "未启动");
  $("runtimeStatus").textContent = running ? "运行中" : (r.status === "exited" ? "已退出" : "已停止"); $("runtimeStatus").className = running ? "running" : "";
  $("runtimePid").textContent = r.pid ? "PID " + r.pid : "等待启动"; $("runtimeRunDir").textContent = r.run_dir ? r.run_dir.split(/[\\/]/).pop() : "—"; $("runtimeRunDir").title = r.run_dir || "";
  $("runtimeLastMessage").textContent = r.last_message || "每次启动自动隔离"; const since = Number(r.started_at_ms || 0);
  $("runtimeSince").textContent = since ? "开始于 " + new Date(since).toLocaleTimeString() : "尚未运行"; $("runtimeAge").textContent = since && running ? formatAge(Date.now() - since) : "—";
  $("startMode").disabled = running; $("stopMode").disabled = !running;
}
async function refreshRuntimes() { try { const data = await api("/api/runtimes"); runtimes = Object.fromEntries((data.modes || []).map((x) => [x.mode, x])); updateRuntimeCards(); } catch (e) { notify("运行态刷新失败：" + e.message, "error"); } }
function runtimePayload(mode) {
  if (mode === "simulation") return { mode: mode, capital_cny: $("simulationCapital").value, symbols: $("simulationSymbols").value, duration_secs: Number($("simulationDuration").value) };
  if (mode === "backtest") return { mode: mode, input: $("backtestInput").value, anchors: $("backtestAnchors").value, max_position: Number($("backtestMaxPosition").value), quantity: Number($("backtestQuantity").value), queue_ahead: Number($("backtestQueue").value), market_to_decision_ms: Number($("backtestDecision").value), decision_to_exchange_ms: 0, require_flat_at_end: true };
  return { mode: mode, duration_secs: 0, max_position: 1, quantity: 1, entry_threshold_bps: 0, allow_orders: $("allowOrders").checked };
}
async function startMode() { $("startMode").disabled = true; notify("正在启动" + modeMeta[currentMode].label + "…"); try { const r = await api("/api/runtime/start", { method: "POST", body: JSON.stringify(runtimePayload(currentMode)) }); notify(r.message + " · PID " + r.pid, "ok"); await refreshRuntimes(); await refreshModeData(); } catch (e) { notify("启动失败：" + e.message, "error"); updateRuntimeCards(); } }
async function stopMode() { $("stopMode").disabled = true; notify("正在停止" + modeMeta[currentMode].label + "…"); try { const r = await api("/api/runtime/stop", { method: "POST", body: JSON.stringify({ mode: currentMode }) }); notify(r.message, "ok"); await refreshRuntimes(); await refreshModeData(); } catch (e) { notify("停止失败：" + e.message, "error"); updateRuntimeCards(); } }
async function loadLogs() { try { const r = await api("/api/logs/" + currentMode); const pieces = [r.stdout, r.stderr].filter(Boolean); if (pieces.length) $("modeLogs").textContent = pieces.join("\n--- stderr ---\n"); } catch (_) {} }
async function refreshModeData() { if (logTimer) clearTimeout(logTimer); if (currentMode === "simulation") await refreshSimulationMetrics(); if (currentMode === "backtest") await refreshBacktestReport(); await loadLogs(); logTimer = setTimeout(refreshModeData, 2500); }

async function refreshSimulationMetrics() { try { renderSimulationMetrics(await api("/api/metrics/simulation")); } catch (_) {} }
function renderSimulationMetrics(data) {
  const s = data.summary || {}; const source = data.source === "systemd_batch" ? "systemd 批处理" : "控制台子进程";
  const gates = Object.entries(s.gate_rejections || {}).sort((a, b) => Number(b[1]) - Number(a[1])).slice(0, 2).map(([k, v]) => k + " " + number(v)).join(" · ");
  $("simulationNetPnl").textContent = signed(s.net_pnl_ticks); $("simulationFills").textContent = number(s.fill_count) + " / " + number(s.order_count); $("simulationFillHint").textContent = "成交量 " + number(s.filled_quantity) + " · 事件 " + number(s.event_count) + (gates ? " · 门禁 " + gates : "");
  $("simulationRejected").textContent = number(s.rejected_entries); $("simulationPosition").textContent = number(s.current_absolute_position); $("simulationFlatHint").textContent = s.flat_at_end ? "当前无持仓/挂单" : number(s.working_orders) + " 个挂单";
  const run = data.run_id ? " · " + data.run_id : ""; const experiment = data.experiment_id ? " · " + data.experiment_id : "";
  $("simulationUpdated").textContent = "更新 " + new Date(Number(data.observed_at_ms || Date.now())).toLocaleTimeString() + " · " + source + experiment + run + " · 历史 " + (data.history || []).length;
  renderSimulationPnl(data.history || []); renderSymbolBars(data.symbols || []);
  $("simulationRows").innerHTML = (data.symbols || []).map((x) => { const win = x.fills ? (Number(x.winning_fills || 0) / Number(x.fills) * 100).toFixed(1) + "%" : "—"; return "<tr><td>" + escapeHtml(x.symbol) + "<small>" + escapeHtml(x.calendar_state || "") + "</small></td><td>" + signed(x.net_pnl_ticks) + "</td><td>" + signed(x.market_pnl_ticks) + "</td><td>" + signed(x.strategy_pnl_ticks) + "</td><td>" + signed(x.funding_pnl_ticks) + "</td><td>" + signed(x.fees_ticks) + "</td><td>" + win + "</td><td>" + number(x.position) + "</td></tr>"; }).join("") || "<tr><td colspan='8'>尚未收到标的指标</td></tr>";
}
function renderSimulationPnl(history) {
  const svg = $("simulationPnlChart"); const width = 720, height = 230, left = 38, right = 12, top = 22, bottom = 22; svg.innerHTML = "";
  if (!history.length) { svg.innerHTML = "<text x='12' y='32'>等待历史快照…</text>"; return; }
  const values = history.flatMap((p) => ["market_pnl_ticks", "strategy_pnl_ticks", "net_pnl_ticks"].map((k) => Number(p[k] || 0))); const lo = Math.min(0, ...values), hi = Math.max(0, ...values, 1);
  const x = (i) => left + (history.length === 1 ? 0 : i * (width - left - right) / (history.length - 1)); const y = (v) => top + (hi - v) * (height - top - bottom) / Math.max(1, hi - lo);
  const series = [["市场","market_pnl_ticks","#62a8ff"],["策略","strategy_pnl_ticks","#46dfbd"],["净收益","net_pnl_ticks","#ff8090"]];
  svg.innerHTML = "<line x1='" + left + "' x2='" + (width - right) + "' y1='" + y(0) + "' y2='" + y(0) + "' stroke='#35465f' stroke-dasharray='3 3'/>" + series.map((v,i) => "<polyline points='" + history.map((p,n) => x(n) + "," + y(Number(p[v[1]] || 0))).join(" ") + "' fill='none' stroke='" + v[2] + "' stroke-width='2'/><text x='" + (left + i * 100) + "' y='13' fill='" + v[2] + "'>" + v[0] + "</text>").join("");
}
function renderSymbolBars(symbols) {
  const svg = $("simulationSymbolChart"); const width = 720, row = 28, zero = 325, max = Math.max(1, ...symbols.map((x) => Math.abs(Number(x.net_pnl_ticks || 0)))); svg.setAttribute("viewBox", "0 0 " + width + " " + Math.max(120, symbols.length * row + 20));
  svg.innerHTML = "<line x1='" + zero + "' x2='" + zero + "' y1='4' y2='" + Math.max(110, symbols.length * row + 12) + "' stroke='#526782'/>" + symbols.map((x,i) => { const v = Number(x.net_pnl_ticks || 0), w = Math.abs(v) / max * 275, left = v >= 0 ? zero : zero - w, color = v >= 0 ? "#46dfbd" : "#ff8090"; return "<text x='4' y='" + (12 + i * row) + "'>" + escapeHtml(x.symbol) + "</text><rect x='" + left + "' y='" + (3 + i * row) + "' width='" + w + "' height='16' fill='" + color + "'/><text x='" + Math.min(width - 55, v >= 0 ? left + w + 6 : left - 52) + "' y='" + (12 + i * row) + "' fill='" + color + "'>" + signed(v) + "</text>"; }).join("");
}
async function refreshBacktestReport() { try { const data = await api("/api/metrics/backtest"); $("backtestReport").textContent = JSON.stringify(data, null, 2); $("backtestUpdated").textContent = "更新 " + new Date().toLocaleTimeString(); } catch (_) {} }
async function refreshStatus() { try { const s = await api("/api/status"); $("allowProduction").checked = s.allow_production; $("allowOrders").checked = s.allow_order_submission; $("symbol").value = s.symbol; $("environment").value = s.environment; $("confirmation").disabled = !(s.allow_production && s.allow_order_submission); } catch (e) { notify("会话状态刷新失败：" + e.message, "error"); } }
function credentialRequest() { return { environment: $("environment").value, api_key: $("apiKey").value, api_secret: $("apiSecret").value }; }
async function postCredential(path) { try { const r = await api(path, { method: "POST", body: JSON.stringify(credentialRequest()) }); setMessage(r.message, "ok"); notify(r.message, "ok"); $("apiKey").value = ""; $("apiSecret").value = ""; await refreshStatus(); } catch (e) { setMessage(e.message, "error"); notify(e.message, "error"); } }
function bindEvents() {
  document.querySelectorAll(".mode-link").forEach((b) => b.addEventListener("click", () => switchMode(b.dataset.mode)));
  $("startMode").addEventListener("click", startMode); $("stopMode").addEventListener("click", stopMode); $("refreshMode").addEventListener("click", refreshModeData);
  $("refreshAll").addEventListener("click", async () => { await refreshRuntimes(); await refreshStatus(); await refreshModeData(); });
  $("environment").addEventListener("change", () => { const prod = $("environment").value === "production"; $("allowProduction").checked = prod; $("allowOrders").checked = false; $("confirmation").disabled = !prod; if (!prod) $("confirmation").value = ""; });
  const toggleConfirmation = () => { $("confirmation").disabled = !($("allowProduction").checked && $("allowOrders").checked); }; $("allowProduction").addEventListener("change", toggleConfirmation); $("allowOrders").addEventListener("change", toggleConfirmation);
  $("saveSession").addEventListener("click", async () => { const payload = Object.assign(credentialRequest(), { allow_production: $("allowProduction").checked, allow_order_submission: $("allowOrders").checked, confirmation: $("confirmation").value, symbol: $("symbol").value, proxy: $("proxy").value }); try { const r = await api("/api/session", { method: "POST", body: JSON.stringify(payload) }); setMessage(r.message, "ok"); notify(r.message, "ok"); $("apiKey").value = ""; $("apiSecret").value = ""; await refreshStatus(); } catch (e) { setMessage(e.message, "error"); notify(e.message, "error"); } });
  $("saveCredentials").addEventListener("click", () => postCredential("/api/credentials/save")); $("deleteCredentials").addEventListener("click", () => postCredential("/api/credentials/delete"));
  $("clearSession").addEventListener("click", async () => { try { const r = await api("/api/session/clear", { method: "POST", body: "{}" }); setMessage(r.message, "ok"); notify(r.message, "ok"); await refreshStatus(); } catch (e) { notify(e.message, "error"); } });
  $("clearLog").addEventListener("click", () => { $("modeLogs").textContent = "暂无运行日志"; $("modeLogs").classList.remove("error"); });
  document.querySelectorAll(".check-card").forEach((b) => b.addEventListener("click", async () => { b.disabled = true; const label = b.querySelector("b").textContent; notify("开始：" + label); try { const r = await api(b.dataset.endpoint, { method: "POST", body: "{}" }); notify(r.message || "完成", "ok"); } catch (e) { notify(label + "失败：" + e.message, "error"); } finally { b.disabled = false; } }));
}
bindEvents(); switchMode("live"); refreshStatus(); refreshRuntimes();
setInterval(() => { refreshRuntimes(); if (currentMode === "simulation") refreshSimulationMetrics(); if (currentMode === "backtest") refreshBacktestReport(); loadLogs(); }, 2000);
setInterval(updateRuntimeCards, 1000);