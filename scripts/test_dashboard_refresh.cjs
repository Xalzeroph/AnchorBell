const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

test('slow dashboard responses cannot create overlapping refresh batches', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../engine/web/app.js'), 'utf8');
  const start = source.indexOf('async function load()');
  const end = source.indexOf('\ndocument.querySelectorAll', start);
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const requests = [];
  const state = { series: { health: [], metrics: [] } };
  const context = vm.createContext({
    AbortController, setTimeout, clearTimeout,
    state, api: async name => { requests.push(name); await gate; return { marker: name }; },
    values: () => [], $: () => ({}), render: () => {}, notice: () => {},
  });
  vm.runInContext('let loading=false;\n' + source.slice(start, end), context);
  const first = context.load();
  const overlapping = context.load();
  assert.equal(requests.length, 9, 'only one batch may be pending');
  release();
  await Promise.all([first, overlapping]);
  assert.equal(state.data.status.marker, 'status');
  await context.load();
  assert.equal(requests.length, 18, 'refresh must resume after completion');
});

for (const stalledStage of ['fetch', 'body']) {
  test(`refresh recovers when the ${stalledStage} stalls until timeout`, async () => {
    const source = fs.readFileSync(path.join(__dirname, '../engine/web/app.js'), 'utf8');
    const api = source.slice(source.indexOf('async function api('), source.indexOf('\n', source.indexOf('async function api(')));
    const start = source.indexOf('async function load()');
    const load = source.slice(start, source.indexOf('\ndocument.querySelectorAll', start));
    let timeout;
    let stalled = true;
    let requests = 0;
    const state = { data: { previous: true }, series: { health: [], metrics: [] } };
    const context = vm.createContext({
      state, AbortController,
      setTimeout: (callback, delay) => { assert.equal(delay, 15000); timeout = callback; return 1; },
      clearTimeout: () => {},
      fetch: async (_url, { signal }) => {
        requests++;
        const waitForAbort = () => new Promise((resolve, reject) => {
          if (signal?.aborted) reject(new Error('aborted'));
          else signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
        });
        if (stalled && stalledStage === 'fetch') return waitForAbort();
        return { ok: true, json: () => stalled ? waitForAbort() : Promise.resolve({ recovered: true }) };
      },
      values: () => [], $: () => ({}), render: () => {}, notice: () => {},
    });
    vm.runInContext('let loading=false;\n' + api + '\n' + load, context);
    const first = context.load();
    await Promise.resolve();
    assert.equal(typeof timeout, 'function', 'refresh must have a bounded deadline');
    timeout();
    await first;
    assert.equal(state.data.previous, true, 'timeout must preserve the last successful snapshot');
    stalled = false;
    await context.load();
    assert.equal(requests, 18);
    assert.equal(state.data.status.recovered, true);
  });
}
