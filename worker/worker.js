/**
 * XDC Open Data: live feed Worker (part of XDC Outpost)
 * https://xdcoutpost.xyz/XDC_Open_Data/
 *
 * Every 5 minutes (Cron Trigger) this Worker:
 *   1. reads the RPC list that XDC Atlas already maintains,
 *   2. checks every HTTPS endpoint server-side (no browser CORS problems),
 *   3. reads a network snapshot (block, block time, gas, epoch progress),
 *   4. saves the result to one Workers KV key ("state").
 *
 * Anyone can then GET (free, no API key, CORS open):
 *   /                              index of everything this Worker serves
 *   /v1/rpc-health/latest.json     endpoint status, lag, latency, 24 h uptime
 *   /v1/network/latest.json        block, block time, gas price, epoch progress
 *   /v1/status.json                is this feed itself healthy?
 *
 * Setup: one KV namespace bound as STATE and one Cron Trigger, every 5 minutes. Both can be
 * set in the Cloudflare dashboard; wrangler.toml holds the same settings for the CLI.
 *
 * Fits the Workers Free plan: 288 runs/day, 1 KV write per run (limit 1,000/day), and
 * at most 47 requests per run (limit 50; see MAX_PROBES). Only standard eth_* RPC methods
 * are used (no XDPoS_* calls): the epoch and round come from block headers.
 */

const CONFIG = {
  RPC_LIST_URL: "https://xdcoutpost.xyz/XDC_Atlas/data/rpcs.json",
  DOCS_URL: "https://xdcoutpost.xyz/XDC_Open_Data/",
  CATALOG_URL: "https://xdcoutpost.xyz/XDC_Open_Data/v1/index.json",
  SCHEMA_BASE: "https://xdcoutpost.xyz/XDC_Open_Data/v1/schemas/",
  SOURCE: "XDC Outpost (xdcoutpost.xyz)",
  LICENSE: "CC-BY-4.0",
  INTERVAL_S: 300,          // must match the Cron Trigger
  TIMEOUT_MS: 6000,         // per endpoint check
  CONCURRENCY: 6,           // Workers allow 6 open connections at once
  // The free plan allows 50 requests per run. Worst case here: 26 checks + 1 list + 18 snapshot calls
  // (2 networks x 3 calls x 3 tries) + 2 KV calls (counted to be safe) = 47, leaving 3 spare for redirects.
  MAX_PROBES: 26,
  LAG_DEGRADED: 10,         // blocks behind the network before "degraded" (~20 s)
  LATENCY_DEGRADED_MS: 3000,
  ROUNDS_PER_EPOCH: 900,
  BLOCK_TIME_SAMPLE: 100,   // blocks used for the average block time
  MEMORY_CACHE_MS: 30000,   // reuse the KV read inside one Worker instance
  USER_AGENT: "XDC-Open-Data/1.0 (+https://xdcoutpost.xyz/XDC_Open_Data/)",
  // Used first for the network snapshot, then the healthiest checked endpoints.
  PRIMARY_RPC: {
    mainnet: "https://rpc.plugin.global/xdcmainnet",
    apothem: "https://rpc.plugin.global/xdcapothem",
  },
};

// Only used if the Atlas list can't be fetched and nothing is cached yet.
const FALLBACK_NETWORKS = [
  { network: "mainnet", chain_id: 50, endpoints: [
    { url: "https://rpc.plugin.global/xdcmainnet", provider: "Plugin", privacy: "none" },
    { url: "https://rpc.xdc.org", provider: "XDC Foundation", privacy: "unspecified" },
    { url: "https://erpc.xinfin.network", provider: "XinFin", privacy: "unspecified" },
    { url: "https://rpc.xinfin.network", provider: "XinFin", privacy: "unspecified" },
    { url: "https://rpc.ankr.com/xdc", provider: "Ankr", privacy: "limited" },
  ] },
  { network: "apothem", chain_id: 51, endpoints: [
    { url: "https://rpc.plugin.global/xdcapothem", provider: "Plugin", privacy: "none" },
    { url: "https://erpc.apothem.network", provider: "XinFin", privacy: "unspecified" },
    { url: "https://rpc.apothem.network", provider: "XinFin", privacy: "unspecified" },
  ] },
];

const NETWORK_KEYS = { 50: "mainnet", 51: "apothem" };

/* ------------------------------------------------------------------ */
/* Entry points                                                        */
/* ------------------------------------------------------------------ */

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(runChecks(env));
  },
  async fetch(request, env) {
    return handleRequest(request, env);
  },
};

/* ------------------------------------------------------------------ */
/* Scheduled run                                                       */
/* ------------------------------------------------------------------ */

async function runChecks(env, now = () => Date.now()) {
  const started = now();
  const errors = [];
  const notes = [];
  const prev = (await env.STATE.get("state", "json")) || {};

  // 1. Endpoint list: Atlas, else last good copy, else built-in.
  let networks, listSource;
  try {
    networks = parseAtlasList(await fetchJson(CONFIG.RPC_LIST_URL, 8000));
    listSource = "xdc-atlas";
  } catch (e) {
    errors.push("Could not read the Atlas RPC list: " + msg(e));
    if (prev.rpc_list && prev.rpc_list.length) { networks = prev.rpc_list; listSource = "xdc-atlas (cached copy)"; }
    else { networks = FALLBACK_NETWORKS; listSource = "built-in"; }
  }

  // 2. Probe every HTTPS endpoint, 6 at a time.
  let jobs = [];
  for (const net of networks) for (const ep of net.endpoints) if (ep.url.startsWith("https://")) jobs.push({ net, ep });
  const skipped = new Set();
  if (jobs.length > CONFIG.MAX_PROBES) {
    // Rotate which endpoints are left out, so every endpoint is still checked regularly.
    const start = ((prev.runs || 0) * CONFIG.MAX_PROBES) % jobs.length;
    const keep = new Set(Array.from({ length: CONFIG.MAX_PROBES }, (_, i) => (start + i) % jobs.length));
    jobs.forEach((j, i) => { if (!keep.has(i)) skipped.add(j.ep.url); });
    jobs = jobs.filter(j => !skipped.has(j.ep.url));
    notes.push(`${skipped.size} endpoint(s) not checked this run (request limit); they are checked in later runs`);
  }
  const results = await pool(jobs, CONFIG.CONCURRENCY, j => probe(j.ep.url, now));
  const byUrl = new Map(jobs.map((j, i) => [j.ep.url, results[i]]));

  const t = now();
  const generatedAt = new Date(t).toISOString();
  const buckets = { ...Object.fromEntries([...skipped].filter(u => (prev.buckets || {})[u]).map(u => [u, prev.buckets[u]])),
                    ...updateBuckets(prev.buckets || {}, jobs, byUrl, t) };

  const healthNets = networks.map(net => buildHealthNetwork(net, byUrl, buckets, skipped));

  // 3. Network snapshot per network, with failover.
  const netSnaps = [];
  for (const hn of healthNets) {
    const order = unique([CONFIG.PRIMARY_RPC[hn.network], ...hn.recommended, ...hn.endpoints.filter(e => e.status !== "down").map(e => e.url)]).filter(Boolean);
    try { netSnaps.push(await networkSnapshot(hn, order)); }
    catch (e) {
      errors.push(`${hn.network} snapshot failed: ${msg(e)}`);
      netSnaps.push({ network: hn.network, chain_id: hn.chain_id, status: "down", error: msg(e) });
    }
  }

  const meta = schema => ({
    $schema: CONFIG.SCHEMA_BASE + schema,
    version: "1",
    generated_at: generatedAt,
    update_interval_seconds: CONFIG.INTERVAL_S,
    stale_after_seconds: CONFIG.INTERVAL_S * 3,
    source: CONFIG.SOURCE,
    docs: CONFIG.DOCS_URL,
    license: CONFIG.LICENSE,
  });

  const rpcHealth = {
    ...meta("rpc-health.v1.json"),
    checked_from: "Cloudflare Workers (server-side, so browser CORS rules do not affect results)",
    check_method: "HTTPS POST eth_blockNumber",
    endpoint_list_source: listSource,
    thresholds: {
      timeout_ms: CONFIG.TIMEOUT_MS,
      degraded_lag_blocks: CONFIG.LAG_DEGRADED,
      degraded_latency_ms: CONFIG.LATENCY_DEGRADED_MS,
    },
    networks: healthNets,
  };
  const network = { ...meta("network.v1.json"), networks: netSnaps };
  const status = {
    $schema: CONFIG.SCHEMA_BASE + "status.v1.json",
    version: "1",
    last_run_at: generatedAt,
    run_duration_ms: now() - started,
    update_interval_seconds: CONFIG.INTERVAL_S,
    endpoints_checked: jobs.length,
    endpoint_list_source: listSource,
    ok: errors.length === 0,
    errors,
    notes,
  };

  const state = { rpc_health: rpcHealth, network, status, buckets, rpc_list: networks, runs: (prev.runs || 0) + 1 };
  await env.STATE.put("state", JSON.stringify(state));
  memo = { at: 0, state: null }; // next request in this instance re-reads
  return state;
}

/* ------------------------------------------------------------------ */
/* Endpoint list                                                       */
/* ------------------------------------------------------------------ */

function parseAtlasList(j) {
  if (!j || !Array.isArray(j.networks)) throw new Error("unexpected format");
  // One entry per network and one per URL, whatever the list contains: the request budget
  // (see MAX_PROBES) assumes at most two networks.
  const byKey = new Map();
  for (const n of j.networks) {
    const key = n && NETWORK_KEYS[n.chainId];
    if (!key || !Array.isArray(n.endpoints)) continue;
    if (!byKey.has(key)) byKey.set(key, { network: key, chain_id: n.chainId, endpoints: [] });
    const net = byKey.get(key);
    for (const e of n.endpoints) {
      if (!e || typeof e.url !== "string" || net.endpoints.some(x => x.url === e.url)) continue;
      net.endpoints.push({
        url: e.url, provider: e.provider || null, privacy: e.privacy || "unspecified",
        ...(e.recommended ? { recommended_by_atlas: true } : {}),
      });
    }
  }
  const out = [...byKey.values()];
  if (!out.length) throw new Error("no known networks in list");
  return out;
}

/* ------------------------------------------------------------------ */
/* Probing                                                             */
/* ------------------------------------------------------------------ */

async function probe(url, now) {
  const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_blockNumber", params: [] });
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), CONFIG.TIMEOUT_MS);
  const t0 = now();
  try {
    const r = await fetch(url, { method: "POST", body, signal: ctl.signal,
      headers: { "content-type": "application/json", "user-agent": CONFIG.USER_AGENT } });
    const ms = now() - t0;
    if (!r.ok) return { ok: false, ms, error: "HTTP " + r.status };
    let j;
    try { j = await r.json(); } catch { return { ok: false, ms, error: "Reply was not JSON" }; }
    if (j && typeof j.result === "string" && /^0x[0-9a-f]+$/i.test(j.result)) {
      return { ok: true, ms, block: parseInt(j.result, 16) };
    }
    return { ok: false, ms, error: (j && j.error && j.error.message) ? "RPC error: " + String(j.error.message).slice(0, 120) : "No block number returned" };
  } catch (e) {
    return { ok: false, ms: null, error: e && e.name === "AbortError" ? `No answer within ${CONFIG.TIMEOUT_MS / 1000} s` : "Connection failed: " + msg(e) };
  } finally { clearTimeout(timer); }
}

async function pool(items, n, fn) {
  const out = new Array(items.length);
  let next = 0;
  const worker = async () => { while (next < items.length) { const i = next++; out[i] = await fn(items[i]); } };
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, worker));
  return out;
}

/** Network head: the highest block that at least two endpoints agree on (within the lag limit). */
function consensusBest(blocks) {
  const b = [...blocks].sort((x, y) => y - x);
  if (!b.length) return null;
  const need = Math.min(2, b.length);
  for (const cand of b) if (b.filter(x => Math.abs(x - cand) <= CONFIG.LAG_DEGRADED).length >= need) return cand;
  return b[0];
}

/* 24 h uptime kept as hourly buckets per URL: [hourStartMs, answered, checks, latencySumMs]. */
function updateBuckets(old, jobs, byUrl, t) {
  const hour = Math.floor(t / 3600000) * 3600000;
  const cutoff = hour - 23 * 3600000;
  const out = {};
  for (const { ep } of jobs) {
    const r = byUrl.get(ep.url);
    const list = (old[ep.url] || []).filter(b => b[0] >= cutoff).map(b => b.slice());
    let cur = list.find(b => b[0] === hour);
    if (!cur) { cur = [hour, 0, 0, 0]; list.push(cur); }
    cur[2] += 1;
    if (r.ok) { cur[1] += 1; cur[3] += r.ms || 0; }
    out[ep.url] = list;
  }
  return out;
}

function buildHealthNetwork(net, byUrl, buckets, skipped = new Set()) {
  const https = net.endpoints.filter(e => e.url.startsWith("https://") && !skipped.has(e.url));
  const best = consensusBest(https.map(e => byUrl.get(e.url)).filter(r => r.ok).map(r => r.block));

  const endpoints = https.map(e => {
    const r = byUrl.get(e.url);
    let status = "down", lag = null, error = r.error || null;
    if (r.ok) {
      lag = best == null ? 0 : best - r.block;
      status = "ok";
      if (lag > CONFIG.LAG_DEGRADED) { status = "degraded"; error = `${lag} blocks behind the network`; }
      else if (lag < -CONFIG.LAG_DEGRADED) { status = "degraded"; error = "Reports a block far ahead of other endpoints (wrong chain?)"; }
      else if (r.ms > CONFIG.LATENCY_DEGRADED_MS) { status = "degraded"; error = `Slow: ${r.ms} ms`; }
      lag = Math.max(0, lag);
    }
    const b = buckets[e.url] || [];
    const answered = b.reduce((s, x) => s + x[1], 0), checks = b.reduce((s, x) => s + x[2], 0), msSum = b.reduce((s, x) => s + x[3], 0);
    return {
      url: e.url,
      provider: e.provider,
      privacy: e.privacy,
      status,
      block_number: r.ok ? r.block : null,
      lag_blocks: lag,
      latency_ms: r.ok ? r.ms : null,
      error,
      uptime_24h_pct: checks ? Math.round((answered / checks) * 1000) / 10 : null,
      avg_latency_24h_ms: answered ? Math.round(msSum / answered) : null,
      checks_24h: checks,
      ...(e.recommended_by_atlas ? { recommended_by_atlas: true } : {}),
    };
  });

  const recommended = endpoints.filter(e => e.status === "ok")
    .sort((a, b) => (a.lag_blocks - b.lag_blocks) || ((b.uptime_24h_pct || 0) - (a.uptime_24h_pct || 0)) || (a.latency_ms - b.latency_ms))
    .slice(0, 3).map(e => e.url);

  return {
    network: net.network,
    chain_id: net.chain_id,
    best_block: best,
    endpoints_checked: endpoints.length,
    endpoints_ok: endpoints.filter(e => e.status === "ok").length,
    endpoints_degraded: endpoints.filter(e => e.status === "degraded").length,
    endpoints_down: endpoints.filter(e => e.status === "down").length,
    recommended,
    endpoints,
    not_checked: net.endpoints.filter(e => !e.url.startsWith("https://") || skipped.has(e.url)).map(e => ({
      url: e.url, provider: e.provider,
      reason: e.url.startsWith("https://") ? "Not checked this run (request limit); checked in a later run"
        : (/^wss?:/i.test(e.url) ? "WebSocket endpoints are not checked yet" : "Only https:// endpoints are checked"),
    })),
  };
}

/* ------------------------------------------------------------------ */
/* Network snapshot                                                    */
/* ------------------------------------------------------------------ */

async function networkSnapshot(hn, urls) {
  const call = (method, params) => rpcFailover(urls.slice(0, 3), method, params);
  const errors = [];
  const soft = p => p.catch(e => { errors.push(msg(e)); return null; });

  // Standard eth_* calls only. Epoch and round come from the block header's extraData.
  const [head, gas] = await Promise.all([
    call("eth_getBlockByNumber", ["latest", false]),
    soft(call("eth_gasPrice", [])),
  ]);
  const headNum = hexNum(head.result.number), headTs = hexNum(head.result.timestamp);
  const past = await soft(call("eth_getBlockByNumber", ["0x" + Math.max(0, headNum - CONFIG.BLOCK_TIME_SAMPLE).toString(16), false]));
  const avgBlockTime = past && past.result ? round2((headTs - hexNum(past.result.timestamp)) / Math.min(CONFIG.BLOCK_TIME_SAMPLE, headNum || 1)) : null;

  let epoch = null;
  const round = roundFromExtra(head.result.extraData);
  if (round != null && V2_SWITCH_BLOCK[hn.network]) {
    const into = round % CONFIG.ROUNDS_PER_EPOCH;
    epoch = {
      number: Math.floor(V2_SWITCH_BLOCK[hn.network] / CONFIG.ROUNDS_PER_EPOCH) + Math.floor(round / CONFIG.ROUNDS_PER_EPOCH),
      round,
      rounds_into_epoch: into,
      rounds_per_epoch: CONFIG.ROUNDS_PER_EPOCH,
      est_seconds_to_next_epoch: avgBlockTime ? Math.round((CONFIG.ROUNDS_PER_EPOCH - into) * avgBlockTime) : null,
    };
  } else {
    errors.push("Could not read the consensus round from the block header");
  }

  let gasWei = null, gasGwei = null;
  if (gas && typeof gas.result === "string") { const g = BigInt(gas.result); gasWei = g.toString(); gasGwei = Number(g) / 1e9; }

  return {
    network: hn.network,
    chain_id: hn.chain_id,
    status: errors.length ? "partial" : "ok",
    rpc_used: head.url,
    block_number: headNum,
    block_hash: head.result.hash,
    block_time: new Date(headTs * 1000).toISOString(),
    avg_block_time_seconds: avgBlockTime,
    gas_price_wei: gasWei,
    gas_price_gwei: gasGwei,
    epoch,
    errors,
  };
}

// XDPoS 2.0: extraData = 0x02 + RLP([round, quorumCert]); epoch = V2 switch block / 900 + round / 900.
const V2_SWITCH_BLOCK = { mainnet: 80370000, apothem: 56828700 };

function roundFromExtra(hex) {
  try {
    const h = String(hex || "").slice(2);
    const b = new Uint8Array(h.length / 2);
    for (let i = 0; i < b.length; i++) b[i] = parseInt(h.substr(i * 2, 2), 16);
    if (!b.length || b[0] !== 2) return null;
    const item = i => {
      const p = b[i];
      if (p < 0x80) return [i, 1, false];
      if (p <= 0xb7) return [i + 1, p - 0x80, false];
      if (p <= 0xbf) { const ll = p - 0xb7; return [i + 1 + ll, beInt(b, i + 1, ll), false]; }
      if (p <= 0xf7) return [i + 1, p - 0xc0, true];
      const ll = p - 0xf7; return [i + 1 + ll, beInt(b, i + 1, ll), true];
    };
    const [s, , isList] = item(1);
    if (!isList) return null;
    const [s2, l2, isList2] = item(s);
    if (isList2) return null;
    return l2 ? beInt(b, s2, l2) : 0;
  } catch { return null; }
}
function beInt(b, start, len) { let v = 0; for (let i = 0; i < len; i++) v = v * 256 + b[start + i]; return v; }

async function rpcFailover(urls, method, params) {
  const errs = [];
  for (const url of urls) {
    try {
      const j = await fetchJson(url, CONFIG.TIMEOUT_MS, { jsonrpc: "2.0", id: 1, method, params });
      if (j && j.result != null) return { url, result: j.result };
      errs.push(`${host(url)}: ${j && j.error ? j.error.message : "empty result"}`);
    } catch (e) { errs.push(`${host(url)}: ${msg(e)}`); }
  }
  throw new Error(`${method} failed (${errs.join("; ") || "no endpoints"})`);
}

/* ------------------------------------------------------------------ */
/* HTTP API                                                            */
/* ------------------------------------------------------------------ */

let memo = { at: 0, state: null };

async function getState(env) {
  if (memo.state && Date.now() - memo.at < CONFIG.MEMORY_CACHE_MS) return memo.state;
  const s = await env.STATE.get("state", "json");
  memo = { at: Date.now(), state: s };
  return s;
}

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, HEAD, OPTIONS",
  "access-control-allow-headers": "*",
  "access-control-max-age": "86400",
};

function json(obj, { status = 200, pretty = false, maxAge = 60, schema = null } = {}) {
  const h = {
    ...CORS,
    "content-type": "application/json; charset=utf-8",
    "cache-control": `public, max-age=${maxAge}`,
    "x-content-type-options": "nosniff",
  };
  if (schema) h["link"] = `<${schema}>; rel="describedby"`;
  return new Response(JSON.stringify(obj, null, pretty ? 2 : 0), { status, headers: h });
}

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname.replace(/\/+$/, "") || "/";
  const pretty = url.searchParams.has("pretty");

  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
  if (request.method !== "GET" && request.method !== "HEAD") {
    return json({ error: "Only GET is supported" }, { status: 405 });
  }

  if (path === "/robots.txt") return new Response("User-agent: *\nAllow: /\n", { headers: { ...CORS, "content-type": "text/plain" } });
  if (path === "/llms.txt") return Response.redirect(CONFIG.DOCS_URL + "llms.txt", 302);

  if (path === "/" || path === "/v1") {
    const base = url.origin;
    return json({
      name: "XDC Open Data (live feed)",
      description: "Free, open JSON about the XDC Network for AI agents and scripts. No API key. This Worker serves the live datasets; slower datasets and history are on GitHub Pages.",
      docs: CONFIG.DOCS_URL,
      catalog: CONFIG.CATALOG_URL,
      license: CONFIG.LICENSE,
      datasets: {
        rpc_health: { url: base + "/v1/rpc-health/latest.json", schema: CONFIG.SCHEMA_BASE + "rpc-health.v1.json", update_interval_seconds: CONFIG.INTERVAL_S },
        network: { url: base + "/v1/network/latest.json", schema: CONFIG.SCHEMA_BASE + "network.v1.json", update_interval_seconds: CONFIG.INTERVAL_S },
        status: { url: base + "/v1/status.json", schema: CONFIG.SCHEMA_BASE + "status.v1.json" },
      },
      tip: "Add ?pretty to any URL for indented JSON.",
    }, { pretty, maxAge: 3600 });
  }

  const routes = {
    "/v1/rpc-health/latest.json": "rpc_health",
    "/v1/network/latest.json": "network",
    "/v1/status.json": "status",
  };
  const key = routes[path];
  if (!key) {
    return json({ error: "Not found", see: url.origin + "/", docs: CONFIG.DOCS_URL }, { status: 404, pretty, maxAge: 300 });
  }

  const state = await getState(env);
  if (!state || !state[key]) {
    return json({ error: "No data yet. The first check runs within 5 minutes of deployment.", docs: CONFIG.DOCS_URL }, { status: 503, maxAge: 30 });
  }

  let body = state[key];
  if (key === "status") {
    const age = Math.round((Date.now() - Date.parse(body.last_run_at)) / 1000);
    body = { ...body, seconds_since_last_run: age, stale: age > CONFIG.INTERVAL_S * 3 };
    return json(body, { pretty, maxAge: 30, schema: body.$schema });
  }
  return json(body, { pretty, schema: body.$schema });
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

async function fetchJson(url, timeoutMs, postBody) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const init = { signal: ctl.signal, headers: { "user-agent": CONFIG.USER_AGENT, accept: "application/json" } };
    if (postBody) { init.method = "POST"; init.body = JSON.stringify(postBody); init.headers["content-type"] = "application/json"; }
    const r = await fetch(url, init);
    if (!r.ok) throw new Error("HTTP " + r.status);
    return await r.json();
  } catch (e) {
    if (e && e.name === "AbortError") throw new Error(`timed out after ${timeoutMs / 1000} s`);
    throw e;
  } finally { clearTimeout(timer); }
}

const msg = e => (e && e.message ? e.message : String(e)).slice(0, 200);
const host = u => { try { return new URL(u).host; } catch { return u; } };
const unique = a => [...new Set(a)];
const round2 = x => Math.round(x * 100) / 100;
const hexNum = h => (typeof h === "number" ? h : parseInt(h, 16));
