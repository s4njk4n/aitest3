#!/usr/bin/env python3
"""
XDC Open Data: hourly builder (run by .github/workflows/update-feed.yml).

Writes static JSON into v1/ for GitHub Pages:
  v1/epoch-rewards/latest.json, history/, daily/ (JSON + CSV), monthly/ (CSV)   rewards per owner per epoch
  v1/validators/latest.json + history/, v1/validators/apothem/                 candidates, owners, stake, status
  v1/yield/latest.json                                                         realised yield (1, 7, 30 days)
  v1/events/latest.json, feed.xml, owners/<owner>.xml, history/              changes between runs (+ optional ntfy)
  v1/clients/latest.json                                                       client versions, upcoming upgrades
  v1/ecosystem/projects.json                                                   projects from XDC Atlas
  v1/rpc-health/ and v1/network/ hourly.json + history/                        hourly copies of the live Worker feed
  v1/index.json, llms.txt, v1/build-status.json                                catalog and run status
Private working state (not part of the published API) lives in state/.

How epoch rewards are measured
------------------------------
XDC epoch rewards are not transfers. The protocol adds them straight to balances
inside the first block of each epoch (the "epoch switch" block N): 90 % of each
node's reward to the node's owner, 0 % to voters, 10 % to the foundation wallet.
So for every owner of a rewarded node:

    reward = balance(N) - balance(N-1)
             - the owner's own transactions in block N (value in and out, fees paid)
             - block N's transaction fees, which XDC pays to the owner of the masternode that produced it
             - XDC moved to or from the owner by contracts in block N (for example a stake withdrawal)

Measured across a single block, this needs only an RPC that can answer
eth_getBalance at past blocks (an archive node). XDC moved by contracts is read
from call traces where an RPC offers debug_traceBlockByNumber, otherwise from the
validator contract's Withdraw events (see block_adjustments). If any configured RPC
also serves eth_getRewardByHash (nodes that keep reward files), the exact
protocol figures are used instead and the method is reported as "reward_files".

Epochs and node sets come from ordinary block headers (round in extraData; the
`validators` and `penalties` fields of each epoch's first block), so no XDPoS_*
RPC methods are needed. Mainnet currently pays masternodes only (protector and
observer tiers are not active), so an owner's reward is the sum over their
masternodes; a per-node figure is exact only for single-node owners.

Standard library only. Usage:
  python3 scripts/build_feed.py                  # normal run
  python3 scripts/build_feed.py --diagnose       # also report which RPCs support which methods
  python3 scripts/build_feed.py --backfill 48    # also fill in 48 epochs before the earliest one done
"""

import csv
import hashlib
import http.client
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
V1 = os.path.join(ROOT, "v1")
STATE = os.path.join(ROOT, "state")
WEI = 10**18
USER_AGENT = "XDC-Open-Data-Builder/1.0 (+https://xdcoutpost.xyz/XDC_Open_Data/)"
PLACEHOLDER = "YOUR-SUBDOMAIN"
COINGECKO_ID = "xdce-crowd-sale"

VALIDATOR_CONTRACT = "0x0000000000000000000000000000000000000088"
SEL_GET_CANDIDATES = "0x06a49fce"        # getCandidates()
SEL_GET_CANDIDATE_CAP = "0x58e7525f"     # getCandidateCap(address)
SEL_GET_CANDIDATE_OWNER = "0xb642facd"   # getCandidateOwner(address)
SEL_GET_VOTER_CAP = "0x302b6872"         # getVoterCap(address candidate, address voter)
SEL_GET_VOTERS = "0x2d15cc04"            # getVoters(address)
CONTRACT_PARAMS = {                      # public variables of the validator contract
    "min_candidate_stake_wei": "0xd55b7dff",      # minCandidateCap()
    "min_voter_stake_wei": "0xf8ac9dd5",          # minVoterCap()
    "max_masternodes": "0xd09f1ab4",              # maxValidatorNumber()
    "candidate_withdraw_delay_blocks": "0xd161c767",  # candidateWithdrawDelay()
    "voter_withdraw_delay_blocks": "0xa9ff959e",      # voterWithdrawDelay()
}
SIGN_TX_TO = "0x0000000000000000000000000000000000000089"        # block signing: applied without the EVM, moves no XDC
RANDOMIZE_TX_TO = "0x0000000000000000000000000000000000000090"   # randomize: runs in the EVM but pays no fee to the producer
FOUNDATION_WALLET = "0x92a289fe95a85c53b8d0d113cbaef0c1ec98ac65"  # Mainnet FoundationWalletAddr (params/config_networks.go)
GOVERNANCE_FIELDS = ("name", "status", "latestSignedBlock", "dataCenter")  # allow-list: nothing else is ever copied
SPLIT = {"owner_pct": 90, "voters_pct": 0, "foundation_pct": 10}   # common/constants.go

with open(os.path.join(ROOT, "config.json")) as f:
    CONFIG = json.load(f)
SCHEMAS = CONFIG["site_url"] + "v1/schemas/"

# Optional private archive RPC, from the PRIVATE_ARCHIVE_RPC environment variable (a GitHub secret): your own
# node, or a provider URL that contains an API key. Its address is never written to a file or the log.
_PUBLIC_RPCS = set(CONFIG.get("mainnet_rpcs", []) + (CONFIG.get("archive_rpcs") or []) + ((CONFIG.get("apothem") or {}).get("rpcs") or []))
PRIVATE_RPCS = list(dict.fromkeys(u for u in re.split(r"[\s,]+", os.environ.get("PRIVATE_ARCHIVE_RPC", "").strip())
                                  if u.startswith("http") and u not in _PUBLIC_RPCS and u.rstrip("/") not in _PUBLIC_RPCS))


def rpc_label(url):
    """How an RPC endpoint is named in anything published or logged."""
    if url in PRIVATE_RPCS:
        return "private RPC" if len(PRIVATE_RPCS) == 1 else f"private RPC {PRIVATE_RPCS.index(url) + 1}"
    return url


def _secret_strings():
    """Every string that would give a private RPC away: the URL, its host (unless a public endpoint in
    config.json shares it) and anything in it that looks like a key."""
    public = " ".join(sorted(_PUBLIC_RPCS))
    out = set()
    for u in PRIVATE_RPCS:
        p = urllib.parse.urlsplit(u)
        out.update({u, u.rstrip("/"), u.split("://", 1)[-1], u.split("://", 1)[-1].rstrip("/")})
        for host in (p.netloc, p.hostname or ""):
            if host and host not in public:
                out.add(host)
        for token in re.split(r"[/?&=:@]", "/".join([p.path, p.query, p.username or "", p.password or ""])):
            if len(token) >= 12:
                out.add(token)
    return sorted((x for x in out if x), key=len, reverse=True)


SECRETS = _secret_strings()


def scrub(text):
    """Remove any trace of a private RPC from text that is about to be published or logged."""
    for x in SECRETS:
        text = text.replace(x, "private-rpc")
    return text


class _ScrubStream:
    """Wraps stdout/stderr so a private RPC can never appear in the public Actions log."""

    def __init__(self, stream):
        self._stream = stream

    def write(self, text):
        return self._stream.write(scrub(text))

    def __getattr__(self, name):
        return getattr(self._stream, name)


def scrub_tree(since):
    """Last line of defence: rewrite any file written in this run that still mentions a private RPC."""
    if not SECRETS:
        return 0
    fixed = 0
    for top in (V1, STATE, os.path.join(ROOT, "llms.txt")):
        paths = [top] if os.path.isfile(top) else [os.path.join(d, f) for d, _, fs in os.walk(top) for f in fs]
        for path in paths:
            try:
                if os.path.getmtime(path) < since - 2:
                    continue
                with open(path, encoding="utf-8") as f:
                    text = f.read()
            except (OSError, UnicodeDecodeError):
                continue
            clean = scrub(text)
            if clean != text:
                write_text(path, clean)
                fixed += 1
    return fixed


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z")


def _timed_out(e):
    return isinstance(e, TimeoutError) or isinstance(getattr(e, "reason", None), TimeoutError)


def _no_answer(e):
    """The server could not be reached or did not answer in time (as opposed to answering with an error)."""
    return _timed_out(e) or isinstance(e, ConnectionError) or (
        isinstance(e, urllib.error.URLError) and not isinstance(e, urllib.error.HTTPError))


def fetch_json(url, body=None, timeout=30, attempts=2):
    """GET (or POST when body is given) and parse JSON. A dropped connection, a server error or a garbled reply
    is retried once. A timeout or a 4xx reply is not: waiting again rarely helps, and callers have other
    endpoints to try."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if data:
        headers["Content-Type"] = "application/json"
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except (OSError, http.client.HTTPException, ValueError) as e:
            last = e   # OSError covers every network failure; ValueError a reply that is not JSON
            if _timed_out(e) or (isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500 and e.code != 429):
                break
            if i + 1 < attempts:
                time.sleep(2 * (i + 1))
    raise last


def to_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    return int(s, 16) if s.lower().startswith("0x") else int(s)


def addr0x(a):
    s = str(a).strip()
    if s[:3].lower() == "xdc":
        s = s[3:]
    elif s[:2].lower() == "0x":
        s = s[2:]
    return "0x" + s.lower()


def wei_to_xdc(wei):
    return float(round(Decimal(wei) / Decimal(WEI), 6))


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_json(path, obj, compact=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        if compact:
            json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
        else:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)  # never leave a half-written file


def write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)  # never leave a half-written file


def write_text_if_changed(path, text):
    old = None
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            old = f.read()
    if old != text:
        write_text(path, text)
        return True
    return False


def meta(schema, interval):
    return {
        "$schema": SCHEMAS + schema,
        "version": "1",
        "generated_at": now_iso(),
        "update_interval_seconds": interval,
        "stale_after_seconds": interval * 3,
        "source": "XDC Outpost (xdcoutpost.xyz)",
        "docs": CONFIG["site_url"],
        "license": CONFIG["license"],
    }


def worker_configured():
    return PLACEHOLDER not in CONFIG["worker_url"]


# ABI helpers for the validator contract (static calls only)
def abi_addr(a):
    return addr0x(a)[2:].rjust(64, "0")


def abi_word_addr(result):
    h = str(result)[2:]
    return "0x" + h[-40:].lower() if len(h) >= 40 else None


def abi_addr_array(result):
    d = str(result or "0x")[2:]
    if len(d) < 128:
        return []
    off = int(d[0:64], 16) * 2
    if len(d) < off + 64:
        return []
    n = min(int(d[off:off + 64], 16), (len(d) - off - 64) // 64)   # never read past the end of the data
    return ["0x" + d[off + 64 + i * 64: off + 128 + i * 64][-40:].lower() for i in range(n)]


ZERO = "0x" + "0" * 40


class RPC:
    """JSON-RPC with failover across endpoints, plus batching when the endpoint allows it."""

    strikes = {}   # this run: how many times in a row each endpoint failed to answer at all

    def __init__(self, urls):
        self.urls = list(urls)
        self.last_url = None   # endpoint that answered the most recent call() or batch()

    @staticmethod
    def ordered(urls):
        """The same endpoints, with any that failed to answer twice in a row in this run moved to the back.
        Without this, one hanging endpoint at the front of a list would add a 30-second wait to every call."""
        return sorted(urls, key=lambda u: RPC.strikes.get(u, 0) >= 2)

    @staticmethod
    def post(url, body):
        """One HTTP request. Any failure is re-raised with a private RPC's address already removed from the
        message, so no caller can log part of it by cutting the text short."""
        try:
            res = fetch_json(url, body)
        except Exception as e:  # noqa: BLE001
            if _no_answer(e):
                RPC.strikes[url] = RPC.strikes.get(url, 0) + 1
            raise RuntimeError(scrub(str(e) or type(e).__name__)) from None
        RPC.strikes.pop(url, None)
        return res

    @staticmethod
    def _error_text(err):
        return scrub(str(err.get("message", err) if isinstance(err, dict) else err))[:160]

    def call_one(self, url, method, params):
        j = self.post(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if isinstance(j, dict) and j.get("error"):
            raise RuntimeError(self._error_text(j["error"]))
        if not isinstance(j, dict) or "result" not in j:
            raise RuntimeError("no result")
        return j["result"]

    def call(self, method, params):
        errors = []
        for url in self.ordered(self.urls):
            try:
                res = self.call_one(url, method, params)
                if res is not None:
                    self.last_url = url
                    return res
                errors.append(f"{rpc_label(url)}: null result")
            except Exception as e:  # noqa: BLE001  (any failure means try the next endpoint)
                errors.append(f"{rpc_label(url)}: {e}")
        raise RuntimeError((f"{method} failed on every RPC: " + "; ".join(errors))[:700])

    def _batch_at(self, url, calls, chunk):
        out = []
        for i in range(0, len(calls), chunk):
            part = calls[i:i + chunk]
            res = self.post(url, [{"jsonrpc": "2.0", "id": k, "method": m, "params": p} for k, (m, p) in enumerate(part)])
            if not isinstance(res, list):
                raise RuntimeError("batch requests not supported")
            by_id = {r.get("id"): r for r in res if isinstance(r, dict)}
            for k in range(len(part)):
                r = by_id.get(k)
                if not r or r.get("error") or r.get("result") is None:
                    raise RuntimeError(self._error_text((r or {}).get("error") or "missing result"))
                out.append(r["result"])
        return out

    def _one_by_one_at(self, url, calls):
        out = []
        for m, p in calls:
            r = self.call_one(url, m, p)
            if r is None:
                raise RuntimeError("null result")
            out.append(r)
        return out

    def batch(self, calls, chunk=50):
        """Run many calls. All results come from one endpoint, so they are consistent. A JSON-RPC batch is tried
        on each endpoint first; only if none of them answers a batch are the calls made one at a time."""
        if not calls:
            return []
        urls = self.ordered(self.urls)
        errors = {}
        for one_by_one in (False, True):
            for url in urls:
                try:
                    out = self._one_by_one_at(url, calls) if one_by_one else self._batch_at(url, calls, chunk)
                    self.last_url = url
                    return out
                except Exception as e:  # noqa: BLE001
                    why = str(e)[:80]
                    if url not in errors:
                        errors[url] = why
                    elif why != errors[url]:
                        errors[url] += f"; one by one: {why}"
        if len(urls) == 1:
            raise RuntimeError(errors[urls[0]])
        raise RuntimeError(("Failed on every RPC: " + "; ".join(f"{rpc_label(u)}: {w}" for u, w in errors.items()))[:700])


def general_rpc(network="mainnet"):
    if network == "mainnet":
        return RPC(CONFIG["mainnet_rpcs"])
    return RPC(CONFIG[network]["rpcs"])


def archive_urls():
    """Endpoints that can read past state. A private RPC comes last here, so it is only used for ordinary
    reads when the public endpoints fail; call traces ask it first (see _trace_block)."""
    return list(dict.fromkeys((CONFIG.get("archive_rpcs") or CONFIG["mainnet_rpcs"]) + PRIVATE_RPCS))


def archive_rpc():
    return RPC(archive_urls())


# --------------------------------------------------------------------------
# History files
# --------------------------------------------------------------------------

def append_history(dataset, date, line, key, sort=False):
    """Add one compact JSON line to v1/<dataset>/history/<date>.ndjson unless one with the same key is there."""
    path = os.path.join(V1, dataset, "history", date + ".ndjson")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for raw in f:
                try:
                    rows.append(json.loads(raw))
                except json.JSONDecodeError:
                    continue
    if any(r.get(key) == line.get(key) for r in rows):
        return False
    rows.append(line)
    if sort:
        rows.sort(key=lambda r: r.get(key))
    write_text(path, "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in rows))
    update_history_index(dataset)
    return True


def update_history_index(dataset, folder_name="history", suffix=".ndjson", schema="history-index.v1.json"):
    folder = os.path.join(V1, dataset, folder_name)
    files = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(suffix) or name == "index.json":
            continue
        entry = {"date": name[:-len(suffix)], "url": f"{CONFIG['site_url']}v1/{dataset}/{folder_name}/{name}"}
        if suffix == ".json" and os.path.exists(os.path.join(folder, name[:-5] + ".csv")):
            entry["csv"] = f"{CONFIG['site_url']}v1/{dataset}/{folder_name}/{name[:-5]}.csv"
        if suffix == ".ndjson":
            with open(os.path.join(folder, name), encoding="utf-8") as f:
                entry["lines"] = sum(1 for line in f if line.strip())
        files.append(entry)
    write_json(os.path.join(folder, "index.json"), {
        "$schema": SCHEMAS + schema,
        "dataset": dataset,
        "format": {".ndjson": "NDJSON: one JSON object per line, oldest first. One file per UTC day.",
                   ".json": "JSON: one file per UTC day (csv: the same as a spreadsheet).",
                   ".csv": "CSV: one file per month, one row per owner per day."}[suffix],
        "files": files,
    })


# --------------------------------------------------------------------------
# Candidate owners
# --------------------------------------------------------------------------

def owners_at(rpc, candidates, tag="latest"):
    """Owner of each candidate according to the validator contract at a block ("latest" or a hex block
    number). A candidate's owner can change (resign, then be proposed again by someone else), so past
    epochs are always read at their own block, never from today's state."""
    candidates = list(candidates)
    res = rpc.batch([("eth_call", [{"to": VALIDATOR_CONTRACT, "data": SEL_GET_CANDIDATE_OWNER + abi_addr(c)}, tag])
                     for c in candidates])
    out = {}
    for c, r in zip(candidates, res):
        o = abi_word_addr(r)
        out[c] = None if (o is None or o == ZERO) else o
    return out


# --------------------------------------------------------------------------
# Price (for accounting: value at the time each epoch was paid)
# --------------------------------------------------------------------------

_PRICE = {}


def current_price():
    """XDC price, only if enabled in config.json. Off by default: CoinGecko's free and standard plans do not
    allow redistributing their data, and this feed is public. When off, no prices or USD values are published."""
    if not (CONFIG.get("prices") or {}).get("enabled"):
        return None
    if "v" not in _PRICE:
        try:
            j = fetch_json(f"https://api.coingecko.com/api/v3/simple/price?ids={COINGECKO_ID}&vs_currencies=usd,aud,eur")
            p = j[COINGECKO_ID]
            _PRICE["v"] = {"usd": p.get("usd"), "aud": p.get("aud"), "eur": p.get("eur"),
                           "source": "CoinGecko", "at": now_iso()}
        except Exception:  # noqa: BLE001  (price is a nice-to-have)
            _PRICE["v"] = None
    return _PRICE["v"]


# --------------------------------------------------------------------------
# Epochs and node sets from ordinary block headers (no XDPoS_* calls)
# --------------------------------------------------------------------------
# XDPoS 2.0 facts used here (from XDPoSChain source):
#  - each block's extraData is 0x02 + RLP([round, quorumCert]); the epoch is
#    SWITCH_EPOCH + round // 900
#  - the first block of each epoch (the "epoch switch" block) is the first block whose
#    round reaches the epoch's first round; only that block has a non-empty `validators`
#    header field (the epoch's masternodes) and `penalties` field (slashed nodes)

CONFIRMATIONS = 20   # an epoch is recorded once its first block is this deep (about 40 seconds)


class EmptyEpoch(RuntimeError):
    """The chain produced no block in this epoch (at least 900 rounds in a row were skipped)."""


NETWORKS = {   # params/config_networks.go
    "mainnet": {"chain_id": 50, "v2_switch_block": 80370000},
    "apothem": {"chain_id": 51, "v2_switch_block": 56828700},
}
ROUNDS_PER_EPOCH = 900
V2_SWITCH_BLOCK = NETWORKS["mainnet"]["v2_switch_block"]
SWITCH_EPOCH = V2_SWITCH_BLOCK // ROUNDS_PER_EPOCH


def _rlp_item(b, i):
    """(payload_start, payload_len, is_list) of the RLP item starting at b[i]."""
    p = b[i]
    if p < 0x80:
        return i, 1, False
    if p <= 0xB7:
        return i + 1, p - 0x80, False
    if p <= 0xBF:
        ll = p - 0xB7
        return i + 1 + ll, int.from_bytes(b[i + 1:i + 1 + ll], "big"), False
    if p <= 0xF7:
        return i + 1, p - 0xC0, True
    ll = p - 0xF7
    return i + 1 + ll, int.from_bytes(b[i + 1:i + 1 + ll], "big"), True


def round_from_extra(extra_hex):
    """Consensus round of an XDPoS 2.0 block, or None for a v1 block."""
    try:
        b = bytes.fromhex(str(extra_hex)[2:])
        if not b or b[0] != 2:
            return None
        s, _, is_list = _rlp_item(b, 1)
        if not is_list:
            return None
        s2, l2, is_list2 = _rlp_item(b, s)
        if is_list2:
            return None
        return int.from_bytes(b[s2:s2 + l2], "big") if l2 else 0
    except (ValueError, IndexError):
        return None


def addresses_from_bytes(h):
    data = str(h or "0x")[2:]
    return ["0x" + data[i:i + 40].lower() for i in range(0, len(data) - 39, 40)]


class Chain:
    """Header reads with a cache, and epoch switch blocks remembered in state/chain.json."""

    def __init__(self, rpc, network="mainnet"):
        self.rpc = rpc
        self.headers = {}
        self.v2_switch = NETWORKS[network]["v2_switch_block"]
        self.switch_epoch = self.v2_switch // ROUNDS_PER_EPOCH
        self.path = os.path.join(STATE, "chain.json" if network == "mainnet" else f"chain-{network}.json")
        self.saved = read_json(self.path) or {"switch_blocks": {}}
        self.fresh = {}   # switch blocks too new to save (they could still be replaced)

    def save(self):
        sb = self.saved["switch_blocks"]
        if len(sb) > 2000:  # keep the most recent
            for k in sorted(sb, key=int)[:-2000]:
                del sb[k]
        write_json(self.path, self.saved)

    def header(self, n):
        if n not in self.headers:
            self.headers[n] = self.rpc.call("eth_getBlockByNumber", [hex(n), False])
        return self.headers[n]

    def head(self):
        h = self.rpc.call("eth_getBlockByNumber", ["latest", False])
        n = to_int(h["number"])
        self.headers[n] = h
        return n

    def round(self, n):
        r = round_from_extra(self.header(n).get("extraData", "0x"))
        if r is None:
            raise RuntimeError(f"block {n} has no XDPoS 2.0 round in extraData")
        return r

    def epoch_of(self, n):
        return self.switch_epoch + self.round(n) // ROUNDS_PER_EPOCH

    def switch_block(self, epoch, head):
        """First block of an epoch: binary search on round (rounds only go up, by at least 1 per block)."""
        sb = self.saved["switch_blocks"]
        if str(epoch) in sb:
            return sb[str(epoch)]
        if epoch in self.fresh:
            return self.fresh[epoch]
        target = (epoch - self.switch_epoch) * ROUNDS_PER_EPOCH
        if target <= 0:
            raise RuntimeError(f"epoch {epoch} is before XDPoS 2.0")
        rh = self.round(head)
        if rh < target:
            raise RuntimeError(f"epoch {epoch} has not started yet")
        lo, hi = max(self.v2_switch + 1, head - (rh - target)), head
        if str(epoch - 1) in sb:
            lo = max(lo, sb[str(epoch - 1)] + 1)
        if str(epoch + 1) in sb:
            hi = min(hi, sb[str(epoch + 1)])   # that block's round is past the target, so the search stays valid
        while lo < hi:
            mid = (lo + hi) // 2
            if self.round(mid) >= target:
                hi = mid
            else:
                lo = mid + 1
        r = self.round(lo)
        if r < target:
            raise RuntimeError(f"could not find the first block of epoch {epoch}")
        if r >= target + ROUNDS_PER_EPOCH:
            raise EmptyEpoch(f"the chain produced no block in epoch {epoch}")
        if head - lo >= CONFIRMATIONS:
            sb[str(epoch)] = lo
        else:
            self.fresh[epoch] = lo
        return lo

    def sets_at(self, n):
        """(masternodes, slashed) written into an epoch switch block's header."""
        h = self.header(n)
        if not addresses_from_bytes(h.get("validators")):
            # Some providers strip (or blank) XDC's extra header fields: ask the others.
            for url in RPC.ordered(self.rpc.urls):
                try:
                    alt = self.rpc.call_one(url, "eth_getBlockByNumber", [hex(n), False])
                except Exception:  # noqa: BLE001
                    continue
                if alt and alt.get("hash") == h.get("hash") and addresses_from_bytes(alt.get("validators")):
                    h = self.headers[n] = alt
                    break
            else:
                raise RuntimeError(f"no RPC returned the masternode list in the header of block {n} "
                                   "(XDC's validators field); is it an epoch's first block?")
        return addresses_from_bytes(h.get("validators")), addresses_from_bytes(h.get("penalties"))


# --------------------------------------------------------------------------
# Epoch rewards
# --------------------------------------------------------------------------

_NO_REWARD_FILES = set()


def exact_rewards(rpc_urls, block_hash):
    """eth_getRewardByHash from any RPC that keeps reward files, else None. Never required.
    An endpoint that has no data once is not asked again in the same run."""
    for url in RPC.ordered(rpc_urls):
        if url in _NO_REWARD_FILES:
            continue
        try:
            res = RPC([url]).call_one(url, "eth_getRewardByHash", [block_hash])
        except Exception:  # noqa: BLE001
            _NO_REWARD_FILES.add(url)
            continue
        if isinstance(res, dict) and res.get("rewards"):
            return res, url
        _NO_REWARD_FILES.add(url)
    return None, None


TRC21_ISSUER = "0x8c0faeb5c6bed2129b8674f262fd45c4e9468bee"   # Mainnet TRC21IssuerSMC (params/config_networks.go)
SEL_TRC21_TOKENS = "0x9d63848a"                                # tokens()
WITHDRAW_TOPIC = "0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568"  # Withdraw(address,uint256,uint256)
_NO_TRACE = set()


def _trace_block(urls, n):
    """Call traces of every transaction in block n (debug_traceBlockByNumber with callTracer) from the first
    endpoint that allows it, as (list, url); (None, None) if none does. A private RPC is asked first.
    Endpoints that refuse are not asked again in the same run."""
    for url in RPC.ordered(PRIVATE_RPCS + [u for u in urls if u not in PRIVATE_RPCS]):
        if url in _NO_TRACE:
            continue
        try:
            res = RPC([url]).call_one(url, "debug_traceBlockByNumber", [hex(n), {"tracer": "callTracer"}])
            if not isinstance(res, list):
                raise RuntimeError("unexpected reply")
            return res, url
        except Exception:  # noqa: BLE001
            _NO_TRACE.add(url)
    return None, None


def _internal_transfers(frame, owners, out, top=True):
    """XDC moved by calls inside a transaction (not the transaction's own value, which is counted separately).
    A frame with an error was reverted together with everything inside it, so it moved nothing.
    Only CALL, CREATE, CREATE2 and SELFDESTRUCT move XDC between accounts: DELEGATECALL and CALLCODE frames
    show a value but run in the caller's own account, and STATICCALL cannot carry value."""
    if not isinstance(frame, dict) or frame.get("error"):
        return
    if not top:
        v = to_int(frame.get("value") or "0x0") or 0
        if v and str(frame.get("type") or "").upper() in ("CALL", "CREATE", "CREATE2", "SELFDESTRUCT"):
            frm, to = addr0x(frame.get("from") or "0x"), addr0x(frame.get("to") or "0x")
            if frm != to:
                if to in owners:
                    out.setdefault(to, [0, 0])[0] += v
                if frm in owners:
                    out.setdefault(frm, [0, 0])[1] += v
    for c in frame.get("calls") or []:
        _internal_transfers(c, owners, out, False)


def block_adjustments(state_rpc, any_rpc, block, owners, producer_owner):
    """Everything other than rewards that changes an owner's balance in block N. Each rule below follows
    XDPoSChain's own code (core/state_processor.go, core/state_transition.go):
      - A transaction to the block-signing address (0x..89) is applied without the EVM and moves no XDC.
      - Any other transaction: the sender pays value (if it succeeded) plus gasUsed x gas price; the recipient
        receives the value. A receipt with gasUsed 0 means the node applied it without the EVM: nothing moved.
      - The fee goes, in full, to the owner of the masternode that produced the block (since block 38,383,838),
        except for randomize transactions (to 0x..90), whose fee is paid to nobody.
      - A transaction to a token registered with the TRC21 issuer is fee-sponsored: the sender pays no fee and
        the producer's owner is paid from the issuer contract instead. The amount is the issuer's balance drop.
      - XDC moved inside contract calls ("internal transfers"): read from call traces when an RPC allows
        debug_traceBlockByNumber. Otherwise stake withdrawals are read from the validator contract's Withdraw
        events (its only way of paying XDC out), and other contract calls are counted as unchecked. An owner
        who sent or received an unchecked call is marked (untraced_call): their figure cannot be trusted.
    state_rpc is the one archive endpoint already shown to read past state (everything that depends on the
    state at a block is asked there); any_rpc may be any endpoint (receipts and traces do not depend on it).
    Returns (adjustments by owner, coverage)."""
    owners = set(owners)
    n = to_int(block["number"])
    coverage = {"method": "none_needed", "contract_txs_unchecked": 0}
    txs = []
    for t in block.get("transactions") or []:
        if not isinstance(t, dict):
            raise RuntimeError(f"block {n} was returned without its transactions")
        to = addr0x(t.get("to") or "0x")
        if to == SIGN_TX_TO:
            continue
        if to == RANDOMIZE_TX_TO and addr0x(t.get("from") or "0x") not in owners:
            continue   # no fee reaches the producer's owner, and neither side is an owner
        txs.append(t)
    if not txs:
        return {}, coverage
    receipts = any_rpc.batch([("eth_getTransactionReceipt", [t["hash"]]) for t in txs])
    rows = []
    for t, rc in zip(txs, receipts):
        if not isinstance(rc, dict):
            raise RuntimeError(f"no receipt for transaction {t.get('hash')}")
        gas_used = to_int(rc.get("gasUsed")) or 0
        if gas_used == 0:
            continue   # applied without the EVM: no value moved and no fee paid
        rows.append({"t": t, "rc": rc, "gas": gas_used, "ok": to_int(rc.get("status", "0x1")) == 1,
                     "from": addr0x(t.get("from") or "0x"), "to": addr0x(t.get("to") or "0x"),
                     "value": to_int(t.get("value")) or 0, "created": not t.get("to")})
    # A plain payment uses exactly 21,000 gas and leaves no logs; anything else may have run contract code.
    for r in rows:
        r["maybe_contract"] = r["created"] or bool(r["rc"].get("logs")) or r["gas"] != 21000

    # Fee-sponsored (TRC21) transactions. The node reads the token list once, from the state before the block.
    sponsored, sponsored_total = set(), 0
    if any(r["maybe_contract"] for r in rows):
        try:
            sponsored = set(abi_addr_array(state_rpc.call("eth_call", [{"to": TRC21_ISSUER, "data": SEL_TRC21_TOKENS}, hex(n - 1)]))) - {ZERO}
        except Exception as e:  # noqa: BLE001
            coverage["trc21_check"] = "failed: " + str(e)[:80]
    for r in rows:
        r["sponsored"] = r["to"] in sponsored
    if any(r["sponsored"] for r in rows):
        b0, b1 = state_rpc.batch([("eth_getBalance", [TRC21_ISSUER, hex(n - 1)]), ("eth_getBalance", [TRC21_ISSUER, hex(n)])])
        sent_in = sum(r["value"] for r in rows if r["to"] == TRC21_ISSUER and r["ok"])
        sponsored_total = max(0, to_int(b0) - to_int(b1) + sent_in)

    adj = {}

    def entry(o):
        return adj.setdefault(o, {"in": 0, "out": 0, "fees_received": 0, "internal_in": 0, "internal_out": 0, "txs": []})

    def touch(o, tx_hash):
        e = entry(o)
        if tx_hash not in e["txs"]:
            e["txs"].append(tx_hash)
        return e

    def untraced(r):
        """A contract call whose inner transfers are unknown: an owner on either end of it is marked."""
        for o in {r["from"], r["to"]} & owners:
            touch(o, r["t"]["hash"])["untraced_call"] = True

    if producer_owner in owners and sponsored_total:
        entry(producer_owner)["fees_received"] += sponsored_total
    for r in rows:
        price = to_int(r["rc"].get("effectiveGasPrice"))
        if price is None:
            price = to_int(r["t"].get("gasPrice")) or 0
        fee = 0 if r["sponsored"] else r["gas"] * price
        if fee and r["to"] != RANDOMIZE_TX_TO and producer_owner in owners:
            entry(producer_owner)["fees_received"] += fee
        if r["ok"] and r["value"] and r["to"] in owners:
            touch(r["to"], r["t"]["hash"])["in"] += r["value"]
        if r["from"] in owners and ((r["value"] if r["ok"] else 0) + fee):
            touch(r["from"], r["t"]["hash"])["out"] += (r["value"] if r["ok"] else 0) + fee

    # Internal transfers: only a successful transaction that ran contract code can make them.
    calls = [r for r in rows if r["ok"] and r["maybe_contract"]]
    if not calls:
        return adj, coverage
    traces, url = _trace_block(any_rpc.urls, n)
    if traces is not None:
        all_hashes = [t["hash"] for t in block["transactions"]]
        by_hash = {}
        for i, tr in enumerate(traces):
            if isinstance(tr, dict):
                h = tr.get("txHash") or (all_hashes[i] if len(traces) == len(all_hashes) else None)
                if h:
                    by_hash[str(h).lower()] = tr
        unchecked = 0
        for r in calls:
            h = r["t"]["hash"]
            frame = (by_hash.get(str(h).lower()) or {}).get("result")
            if not isinstance(frame, dict):
                unchecked += 1
                untraced(r)
                continue
            moved = {}
            _internal_transfers(frame, owners, moved)
            for o, (vin, vout) in moved.items():
                e = touch(o, h)
                e["internal_in"] += vin
                e["internal_out"] += vout
        return adj, {**coverage, "method": "call_traces", "source": rpc_label(url), "contract_txs_unchecked": unchecked}

    # No traces. First, which of these really called a contract? A payment with a note attached also uses
    # more than 21,000 gas, but its recipient has no code.
    tos = sorted({r["to"] for r in calls if not r["created"] and not r["rc"].get("logs")})
    has_code = {}
    if tos:
        try:
            res = state_rpc.batch([("eth_getCode", [a, hex(n - 1)]) for a in tos] + [("eth_getCode", [a, hex(n)]) for a in tos])
            has_code = {a: len(str(res[i])) > 2 or len(str(res[i + len(tos)])) > 2 for i, a in enumerate(tos)}
        except Exception:  # noqa: BLE001  (unknown: keep counting them as contract calls)
            has_code = {}
    calls = [r for r in calls if r["created"] or r["rc"].get("logs") or has_code.get(r["to"], True)]
    if not calls:
        return adj, coverage
    unchecked = 0
    for r in calls:
        for lg in r["rc"].get("logs") or []:
            topics = lg.get("topics") or []
            if addr0x(lg.get("address") or "0x") == VALIDATOR_CONTRACT and topics and str(topics[0]).lower() == WITHDRAW_TOPIC:
                d = str(lg.get("data") or "0x")[2:]
                if len(d) >= 192:   # Withdraw(address _owner, uint256 _blockNumber, uint256 _cap), none indexed
                    o, cap = "0x" + d[24:64].lower(), int(d[128:192], 16)
                    if o in owners:
                        touch(o, r["t"]["hash"])["internal_in"] += cap
        if r["to"] not in (VALIDATOR_CONTRACT, RANDOMIZE_TX_TO):   # those two never pay XDC out any other way
            unchecked += 1
            untraced(r)
    return adj, {**coverage, "method": "validator_withdraw_events", "contract_txs_unchecked": unchecked}


def median_int(values):
    v = sorted(values)
    return v[len(v) // 2] if len(v) % 2 else (v[len(v) // 2 - 1] + v[len(v) // 2]) // 2


NO_ADJUSTMENT = {"in": 0, "out": 0, "fees_received": 0, "internal_in": 0, "internal_out": 0}


def measured_balances(one, owners, n, producer_owner, block):
    """Balance change of each owner across block N, minus everything that is not a reward.
    `one` is a single archive endpoint that has already passed the check in reward_block_state."""
    k = len(owners)
    bal = one.batch([("eth_getBalance", [o, hex(n - 1)]) for o in owners] + [("eth_getBalance", [o, hex(n)]) for o in owners])
    others = [u for u in archive_urls() if u not in one.urls]
    adjustments, coverage = block_adjustments(one, RPC(one.urls + others), block, owners, producer_owner)
    per_owner = {}
    for i, o in enumerate(owners):
        a = adjustments.get(o) or NO_ADJUSTMENT
        per_owner[o] = ((to_int(bal[i + k]) - to_int(bal[i])) - a["in"] + a["out"] - a["fees_received"]
                        - a["internal_in"] + a["internal_out"])
    return per_owner, adjustments, coverage


def reward_block_state(url, n, candidates):
    """(RPC for this one endpoint, owner of each candidate just before block N).
    Everything that depends on the state at a past block is read from ONE archive endpoint, and only after
    checking that it really answers from the block asked for: the foundation wallet is paid 10 % of every
    reward in block N, so an endpoint that reports the same balance for it before and after the block is
    ignoring the block number (it would also report today's owners and zero rewards for everyone)."""
    one = RPC([url])
    f0, f1 = one.batch([("eth_getBalance", [FOUNDATION_WALLET, hex(n - 1)]), ("eth_getBalance", [FOUNDATION_WALLET, hex(n)])])
    if to_int(f0) == to_int(f1):
        raise RuntimeError("returned the same balance before and after the block (not reading past state)")
    # Owners as the node saw them when it paid: the validator contract's state just before block N. A
    # candidate's owner can change, so past epochs are never read from today's state.
    return one, owners_at(one, candidates, hex(n - 1))


UNVERIFIED_ABOVE = 1.2   # with untraced contract calls in the block: more than 20 % above expected is not believed


def build_epoch(epoch, log, chain, head, price=None):
    n = chain.switch_block(epoch, head)
    # Rewards paid in block N are for the epoch two before it. The node finds that epoch by walking back
    # two epoch switch blocks; if the chain skipped a whole epoch in between, the pattern breaks.
    chain.switch_block(epoch - 1, head)
    r_start = chain.switch_block(epoch - 2, head)
    # Who could be rewarded: the masternodes written into the first block of the rewarded epoch.
    eligible, slashed_then = chain.sets_at(r_start)
    if not eligible:
        raise RuntimeError(f"block {r_start} lists no masternodes; is it really an epoch switch block?")

    # The block's contents can come from any endpoint: they are checked against the header already read.
    block = archive_rpc().call("eth_getBlockByNumber", [hex(n), True])
    if to_int(block.get("number")) != n or block.get("hash") != chain.header(n).get("hash"):
        raise RuntimeError(f"RPC endpoints disagree about block {n}; will retry")
    block_hash = block["hash"]
    paid_at = iso(to_int(block["timestamp"]))
    producer = addr0x(block.get("miner") or "0x")
    candidates = list(dict.fromkeys(eligible + ([producer] if len(producer) == 42 else [])))

    exact, exact_url = exact_rewards(list(dict.fromkeys(CONFIG["mainnet_rpcs"] + archive_urls())), block_hash)
    errors = []
    for url in RPC.ordered(archive_urls()):
        try:
            one, owner_of = reward_block_state(url, n, candidates)
            # The foundation wallet receives 10 % of every node's reward in this same block, so if it also owns
            # masternodes its own figure cannot be measured. Those nodes are listed, but get no owner row.
            foundation_nodes = [c for c in eligible if owner_of.get(c) == FOUNDATION_WALLET]
            owners = sorted({owner_of[c] for c in eligible if owner_of.get(c)} - {FOUNDATION_WALLET})
            nodes_of = {o: [c for c in eligible if owner_of.get(c) == o] for o in owners}
            per_owner, node_exact, adjustments = {}, {}, {}
            coverage = {"method": "not_needed_exact_figures", "contract_txs_unchecked": 0}
            if exact:
                method, used = "reward_files", rpc_label(exact_url)
                signers = {addr0x(k): v for k, v in (exact.get("signers") or {}).items()}
                for cand, holders in (exact.get("rewards") or {}).items():
                    cand = addr0x(cand)
                    o = owner_of.get(cand)
                    for holder, amt in (holders or {}).items():
                        if o and addr0x(holder) == o:
                            per_owner[o] = per_owner.get(o, 0) + to_int(amt)
                            node_exact[cand] = node_exact.get(cand, 0) + to_int(amt)
                    if (signers.get(cand) or {}).get("sign") is not None:
                        node_exact.setdefault(cand, 0)
                per_owner = {o: per_owner.get(o, 0) for o in owners}
            else:
                method, used = "balance_change", rpc_label(url)
                per_owner, adjustments, coverage = measured_balances(one, owners, n, owner_of.get(producer), block)
            break
        except Exception as e:  # noqa: BLE001
            errors.append(f"{rpc_label(url)}: {str(e)[:110]}")
    else:
        raise RuntimeError(("no archive RPC could measure the block: " + "; ".join(errors))[:900])

    unchecked = coverage.get("contract_txs_unchecked") or 0

    def untraced(o):
        return bool((adjustments.get(o) or {}).get("untraced_call"))

    # Typical reward for one fully working node: the median of owners who run exactly one node.
    single = [per_owner[o] for o in owners if len(nodes_of[o]) == 1 and per_owner[o] > 0]
    typical = median_int(single) if len(single) >= 3 else None
    if typical is None:
        log.append(f"Epoch {epoch}: only {len(single)} single-node owners were paid, so no typical node reward")

    rows = []
    for o in owners:
        wei, k = per_owner[o], len(nodes_of[o])
        # A figure is unverified when the block held something this feed could not see that may have moved
        # XDC to or from this owner, and there is a sign that it did. Such rows are kept out of every total.
        why = None
        if wei < 0:
            why = ("Negative: XDC left this address inside a contract call in this block that could not be traced "
                   "(for example, the owner is a contract wallet)." if unchecked else
                   "Negative: this address's balance fell in this block in a way its transactions do not explain.")
        elif untraced(o):
            why = ("A contract call that this address sent or received in this block could not be traced, so any XDC "
                   "moved inside that call is mixed into this figure.")
        elif typical and unchecked and wei > typical * k * UNVERIFIED_ABOVE:
            why = ("Higher than expected for this many nodes, and this block had contract calls that could not be "
                   "traced: part of this may be XDC received from a contract rather than a reward.")
        node_rows = []
        for c in nodes_of[o]:
            if why:
                node_rows.append({"candidate": c, "reward_wei": str(wei // k), "basis": "unverified"})
            elif c in node_exact:
                node_rows.append({"candidate": c, "reward_wei": str(node_exact[c]), "basis": "exact"})
            elif k == 1:
                node_rows.append({"candidate": c, "reward_wei": str(wei), "basis": "measured"})
            else:
                node_rows.append({"candidate": c, "reward_wei": str(wei // k), "basis": "equal_share_estimate"})
        for nr in node_rows:
            nr["reward_xdc"] = wei_to_xdc(int(nr["reward_wei"]))
        row = {"owner": o, "reward_wei": str(wei), "reward_xdc": wei_to_xdc(wei), "node_count": k}
        if typical:
            expected = typical * k
            row["expected_wei"] = str(expected)
            row["expected_xdc"] = wei_to_xdc(expected)
            if not why:
                short = max(0, expected - wei)
                row["vs_typical"] = round(wei / expected, 4)
                row["shortfall_xdc"] = wei_to_xdc(short)
                row["shortfall_nodes"] = round(short / typical, 2)
                row["below_typical"] = row["vs_typical"] < 0.98
        if price and price.get("usd") is not None and not why:
            row["value_usd"] = round(wei_to_xdc(wei) * price["usd"], 2)
        row["nodes"] = node_rows
        a = adjustments.get(o) or {}
        if a.get("txs"):
            row["adjusted_for_txs"] = a["txs"]
        if a.get("fees_received"):
            row["block_fees_excluded_xdc"] = wei_to_xdc(a["fees_received"])
        if a.get("internal_in") or a.get("internal_out"):
            row["internal_transfers_excluded_xdc"] = wei_to_xdc(a["internal_in"] - a["internal_out"])
        if why:
            row["unverified"] = True
            row["note"] = why
        rows.append(row)
    rows.sort(key=lambda r: (-int(r["reward_wei"]), r["owner"]))
    unowned = [c for c in eligible if not owner_of.get(c)]

    good = [r for r in rows if not r.get("unverified")]
    total = sum(int(r["reward_wei"]) for r in good)
    totals = {
        "masternodes_eligible": len(eligible),
        "owners": len(rows),
        "owners_paid": sum(1 for r in good if int(r["reward_wei"]) > 0),
        "owners_unverified": len(rows) - len(good),
        "owners_with_several_nodes": sum(1 for r in rows if r["node_count"] > 1),
        "owner_rewards_wei": str(total),
        "owner_rewards_xdc": wei_to_xdc(total),
        "implied_all_rewards_xdc": wei_to_xdc(total * 100 // SPLIT["owner_pct"]),
    }
    if typical:
        totals["owners_below_typical"] = sum(1 for r in good if r.get("below_typical"))
        totals["shortfall_nodes"] = round(sum(r["shortfall_nodes"] for r in good), 2)
    if price and price.get("usd") is not None:
        totals["value_usd"] = round(wei_to_xdc(total) * price["usd"], 2)

    doc = {
        **meta("epoch-rewards.v1.json", 3600),
        "network": "mainnet",
        "chain_id": 50,
        "paid_in_epoch": epoch,
        "rewarded_epoch": epoch - 2,
        "paid_at_block": n,
        "paid_at_block_hash": block_hash,
        "paid_at_time": paid_at,
        "method": method,
        "method_note": (
            "Exact protocol figures from eth_getRewardByHash." if method == "reward_files" else
            "Measured: each owner's balance change across the epoch switch block, minus what is not a reward: the "
            "owner's own transactions, block fees paid to the block producer's owner, and XDC moved by contracts "
            "(see internal_transfers). Rewards are not transfers, so this is how they are measured. An owner with "
            "several nodes receives one combined amount. A row marked unverified could not be fully explained and "
            "is left out of every total."),
        "data_from": used,
        "internal_transfers": {**coverage, "note": {
            "not_needed_exact_figures": "Exact protocol figures were used, so nothing had to be excluded.",
            "none_needed": "This block had no contract calls, so no XDC was moved by contracts.",
            "call_traces": (
                "Every contract call in this block was traced, and XDC it moved to or from an owner was excluded exactly."
                if not unchecked else
                f"Contract calls in this block were traced and the XDC they moved to or from owners was excluded, except "
                f"{unchecked} call(s) the tracer returned nothing for. An owner who sent or received one of those, or "
                "whose figure is negative or far above expected, is marked unverified."),
            "validator_withdraw_events": (
                "No RPC offered call traces for this block, but its only contract calls were to the validator contract, "
                "whose payouts (stake withdrawals) were excluded exactly from its Withdraw events."
                if not unchecked else
                "No RPC offered call traces for this block. Stake withdrawals were still excluded exactly, from the "
                f"validator contract's Withdraw events. {unchecked} other contract call(s) could not be checked: an owner "
                "who sent or received one of them, or whose figure is negative or far above expected, is marked unverified."),
        }[coverage["method"]]},
        "split": SPLIT,
        "price": price,
        "typical_node_reward": None if not typical else {
            "reward_wei": str(typical),
            "reward_xdc": wei_to_xdc(typical),
            "from_single_node_owners": len(single),
            "note": "Median reward of owners running exactly one masternode: what one fully working node earned "
                    "this epoch. Each node's share depends on how many signing transactions it sent.",
        },
        "totals": totals,
        "owners": rows,
        "masternodes_without_owner": unowned,
        "slashed_in_rewarded_epoch": slashed_then,
    }
    if foundation_nodes:
        doc["foundation_wallet_nodes"] = foundation_nodes
    return doc


def summary_line(doc):
    keys = ["paid_in_epoch", "rewarded_epoch", "paid_at_block", "paid_at_time", "method", "price",
            "typical_node_reward", "totals", "slashed_in_rewarded_epoch"]
    line = {k: doc.get(k) for k in keys}
    if line["typical_node_reward"]:
        line["typical_node_reward"] = {k: v for k, v in line["typical_node_reward"].items() if k != "note"}
    return line


def add_to_daily(doc):
    """Per-owner totals per UTC day: what an accountant or tax return needs."""
    date = doc["paid_at_time"][:10]
    path = os.path.join(V1, "epoch-rewards", "daily", date + ".json")
    d = read_json(path) or {"$schema": SCHEMAS + "epoch-rewards-daily.v1.json", "date": date,
                            "epochs": [], "owners": {}}
    if doc["paid_in_epoch"] in d["epochs"]:
        return
    d["epochs"] = sorted(d["epochs"] + [doc["paid_in_epoch"]])
    for r in doc["owners"]:
        w = int(r["reward_wei"])
        if w <= 0 and not r.get("unverified"):
            continue
        o = d["owners"].setdefault(r["owner"], {"reward_wei": "0", "reward_xdc": 0.0, "epochs_paid": 0})
        o["node_count"] = max(o.get("node_count", 0), r["node_count"])
        if r.get("unverified"):   # listed for the owner to check; never added to a total
            o["unverified"] = sorted(o.get("unverified", []) + [{"epoch": doc["paid_in_epoch"], "measured_wei": r["reward_wei"],
                                                                 "measured_xdc": r["reward_xdc"]}], key=lambda u: u["epoch"])
            continue
        o["reward_wei"] = str(int(o["reward_wei"]) + w)
        o["reward_xdc"] = wei_to_xdc(int(o["reward_wei"]))
        o["epochs_paid"] += 1
        if "value_usd" in r:
            o["value_usd"] = round(o.get("value_usd", 0) + r["value_usd"], 2)
            o["epochs_valued"] = o.get("epochs_valued", 0) + 1
    d["owners"] = dict(sorted(d["owners"].items(), key=lambda kv: (-int(kv[1]["reward_wei"]), kv[0])))
    d["generated_at"] = now_iso()
    d["note"] = ("Information only; not tax advice. Totals of rewards credited to each owner address on this UTC day "
                 "(all of the owner's nodes combined). An epoch listed under an owner's unverified is not in that "
                 "owner's totals: its measured figure could not be fully explained, so check it against your own records." + (
                     " value_usd sums each epoch's reward at the price when it was recorded, covering only the epochs "
                     "counted in epochs_valued (epochs filled in later have no price)." if _prices_on() else ""))
    write_json(path, d)
    write_daily_csv(d)
    write_monthly_csv(date[:7])
    update_history_index("epoch-rewards", "daily", ".json", "history-index.v1.json")
    update_history_index("epoch-rewards", "monthly", ".csv", "history-index.v1.json")


SLA_KEEP_DAYS = 35


def update_sla_state(doc):
    """Per UTC day: for each node, epochs as masternode and epochs slashed; for each owner, epochs,
    rewards and how they compared with the typical node. Used for 7- and 30-day history and yield."""
    path = os.path.join(STATE, "sla.json")
    st = read_json(path) or {"days": {}}
    day = st["days"].setdefault(doc["paid_at_time"][:10], {"epochs": [], "times": [], "nodes": {}, "owners": {}, "typical": []})
    if doc["paid_in_epoch"] in day["epochs"]:
        return
    day["epochs"].append(doc["paid_in_epoch"])
    day["times"].append(doc["paid_at_time"])
    t = doc.get("typical_node_reward")
    day["typical"].append(t["reward_wei"] if t else None)
    for r in doc["owners"]:
        for n in r["nodes"]:
            c = day["nodes"].setdefault(n["candidate"], [0, 0])
            c[0] += 1
        if r.get("unverified"):
            continue   # the figure could not be explained: leave this epoch out of the owner's averages
        o = day["owners"].setdefault(r["owner"], {"epochs": 0, "reward_wei": "0", "vs_sum": 0.0, "vs_n": 0,
                                                    "shortfall_nodes": 0.0, "zero": 0, "nodes": 0, "node_epochs": 0,
                                                    "last": 0})
        o["epochs"] += 1
        o["node_epochs"] = o.get("node_epochs", 0) + r["node_count"]
        o["reward_wei"] = str(int(o["reward_wei"]) + max(0, int(r["reward_wei"])))
        if doc["paid_in_epoch"] >= o.get("last", 0):   # epochs can arrive out of order (backfill)
            o["nodes"], o["last"] = r["node_count"], doc["paid_in_epoch"]
        if r.get("vs_typical") is not None:
            o["vs_sum"] += r["vs_typical"]
            o["vs_n"] += 1
            o["shortfall_nodes"] = round(o["shortfall_nodes"] + r["shortfall_nodes"], 4)
        if int(r["reward_wei"]) <= 0:
            o["zero"] += 1
    for c in doc.get("slashed_in_rewarded_epoch") or []:
        day["nodes"].setdefault(c, [0, 0])[1] += 1
    for d in sorted(st["days"])[:-SLA_KEEP_DAYS]:
        del st["days"][d]
    write_json(path, st, compact=True)


def sla_window(st, days, end_date):
    """Merge the per-day state over the `days` UTC days ending at end_date (inclusive)."""
    from datetime import date, timedelta
    end = date.fromisoformat(end_date)
    keys = [(end - timedelta(days=i)).isoformat() for i in range(days)]   # newest day first
    out = {"epochs": 0, "points": [], "typical": [], "nodes": {}, "owners": {}}
    for k in keys:
        d = (st.get("days") or {}).get(k)
        if not d:
            continue
        out["epochs"] += len(d["epochs"])
        out["points"] += list(zip(d["epochs"], d["times"]))
        out["typical"] += d["typical"]
        for c, (m, sl) in d["nodes"].items():
            x = out["nodes"].setdefault(c, [0, 0])
            x[0] += m
            x[1] += sl
        for o, v in d["owners"].items():
            # "nodes" is taken from the first day this owner is seen in, which is the most recent one.
            x = out["owners"].setdefault(o, {"epochs": 0, "reward_wei": 0, "vs_sum": 0.0, "vs_n": 0,
                                             "shortfall_nodes": 0.0, "zero": 0, "nodes": v["nodes"], "node_epochs": 0})
            x["epochs"] += v["epochs"]
            x["node_epochs"] += v.get("node_epochs", v["epochs"] * v["nodes"])
            x["reward_wei"] += int(v["reward_wei"])
            x["vs_sum"] += v["vs_sum"]
            x["vs_n"] += v["vs_n"]
            x["shortfall_nodes"] += v["shortfall_nodes"]
            x["zero"] += v["zero"]
    return out


CSV_COLUMNS = ["date", "owner", "owner_xdc_form", "reward_xdc", "reward_wei", "epochs_paid", "node_count",
               "unverified_epochs", "value_usd", "epochs_valued"]


def _prices_on():
    return bool((CONFIG.get("prices") or {}).get("enabled"))


def _csv_rows(d):
    for owner, o in d["owners"].items():
        row = [d["date"], owner, "xdc" + owner[2:], f"{Decimal(int(o['reward_wei'])) / WEI:.18f}".rstrip("0").rstrip("."),
               o["reward_wei"], o["epochs_paid"], o.get("node_count", ""),
               " ".join(str(u["epoch"]) for u in o.get("unverified", []))]
        if _prices_on():
            row += [o.get("value_usd", ""), o.get("epochs_valued", "")]
        yield row


def _write_csv(path, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CSV_COLUMNS if _prices_on() else CSV_COLUMNS[:-2])
    for r in rows:
        w.writerow(r)
    write_text_if_changed(path, buf.getvalue())


def write_daily_csv(d):
    _write_csv(os.path.join(V1, "epoch-rewards", "daily", d["date"] + ".csv"), _csv_rows(d))


def write_monthly_csv(month):
    folder = os.path.join(V1, "epoch-rewards", "daily")
    rows = []
    for name in sorted(os.listdir(folder)):
        if name.startswith(month) and name.endswith(".json") and name != "index.json":
            rows += list(_csv_rows(read_json(os.path.join(folder, name))))
    _write_csv(os.path.join(V1, "epoch-rewards", "monthly", month + ".csv"), rows)


RUN_EPOCHS = []   # epochs recorded live in this run (for alerts); backfilled epochs are not alerted
RUN_BUDGET_SECONDS = 25 * 60   # no new epoch is started after this long, so a run always finishes and is saved
_RUN = {"deadline": None}


def out_of_time():
    return _RUN["deadline"] is not None and time.monotonic() > _RUN["deadline"]


def record_epoch(doc, set_latest):
    append_history("epoch-rewards", doc["paid_at_time"][:10], summary_line(doc), "paid_in_epoch", sort=True)
    add_to_daily(doc)
    update_sla_state(doc)
    if set_latest:
        write_json(os.path.join(V1, "epoch-rewards", "latest.json"), doc)
        RUN_EPOCHS.append(doc)


def step_rewards(log, backfill=0):
    chain = Chain(general_rpc())
    st_path = os.path.join(STATE, "rewards.json")
    st = read_json(st_path) or {}
    head = chain.head()
    current = chain.epoch_of(head)
    last, first = st.get("last_epoch"), st.get("first_epoch")
    limit = int(CONFIG.get("max_epochs_per_run", 6))

    if last is None:
        targets = [current]
    elif last >= current:
        targets = []
    else:
        start = last + 1
        if current - start > 200:  # far behind after an outage: skip ahead rather than crawl
            log.append(f"Skipped epochs {start}..{current - limit} (too far behind)")
            start = current - limit + 1
        targets = list(range(start, current + 1))[:limit]

    def skip(e, why):
        st["skipped"] = (st.get("skipped") or [])[-49:] + [e]
        log.append(f"Epoch {e} not recorded: {why}")

    done, failed, filled, waiting, late = [], [], [], None, False
    price = current_price()
    try:
        for e in targets:
            if out_of_time():
                late = True
                break
            try:
                if head - chain.switch_block(e, head) < CONFIRMATIONS:
                    waiting = e   # its first block is only seconds old: record it on the next run
                    break
                doc = build_epoch(e, log, chain, head, price)
            except EmptyEpoch as ex:
                skip(e, f"{ex}, so the usual reward pattern does not apply")
                st["last_epoch"] = e
                write_json(st_path, st)
                continue
            except Exception as ex:  # noqa: BLE001
                failed.append(e)
                log.append(f"Epoch {e}: {str(ex)[:900]}")
                break  # keep order: retry from here next run
            record_epoch(doc, set_latest=True)
            st["last_epoch"] = e
            st["first_epoch"] = min(first or e, e)
            first = st["first_epoch"]
            done.append(e)
            write_json(st_path, st)

        if backfill and first:
            for e in range(first - 1, max(first - 1 - backfill, SWITCH_EPOCH + 2), -1):
                if out_of_time():
                    late = True
                    break
                try:
                    doc = build_epoch(e, log, chain, head, None)
                except EmptyEpoch as ex:
                    skip(e, f"{ex}, so the usual reward pattern does not apply")
                    st["first_epoch"] = e
                    write_json(st_path, st)
                    continue
                except Exception as ex:  # noqa: BLE001
                    log.append(f"Backfill stopped at epoch {e}: {str(ex)[:900]}")
                    break
                record_epoch(doc, set_latest=False)
                st["first_epoch"] = e
                filled.append(e)
                write_json(st_path, st)
    finally:
        chain.save()

    if failed and not done:
        raise RuntimeError(f"Could not process epoch {failed[0]} (see notes)")
    parts = []
    if done:
        parts.append(f"Processed epochs {done[0]}..{done[-1]}")
    if filled:
        parts.append(f"backfilled {filled[-1]}..{filled[0]}")
    if waiting is not None:
        parts.append(f"epoch {waiting} has only just started and will be recorded next run")
    if late:
        parts.append("stopped at this run's time limit (the rest continues next run"
                     + ("; run the backfill again for the remainder)" if backfill else ")"))
    if not parts:
        parts.append("Up to date")
    return ", ".join(parts) + f"; chain is at epoch {current}"


# --------------------------------------------------------------------------
# Validators: every candidate, its owner, stake and status
# --------------------------------------------------------------------------

NOT_SIGNING_BLOCKS = 1800   # about an hour without a signature from a masternode


def governance_candidates(log, bases=None):
    """Optional extras from the XDC governance dApp API (MasterNode-App): the last block each node signed,
    and (if switched on in config.json) its registered data centre and name. Never required; returns
    ({}, None) if unreachable."""
    for base in (bases if bases is not None else CONFIG.get("governance_api")) or []:
        try:
            items, page = [], 1
            while page <= 10:
                j = fetch_json(f"{base.rstrip('/')}/candidates?limit=200&page={page}")
                batch = j.get("items") or []
                items += batch
                total = to_int(j.get("total") or 0)
                if not batch or (len(items) >= total if total else len(batch) < 200):
                    break
                page += 1
            out = {}
            for it in items:
                if it.get("candidate"):
                    out[addr0x(it["candidate"])] = it
            if out:
                return out, base
        except Exception as e:  # noqa: BLE001
            log.append(f"Governance API {base} unavailable: {str(e)[:120]}")
    return {}, None


def governance_fields(g, head):
    """Only the allow-listed fields are read. Social links, emails, KYC and anything else the
    governance API returns are never copied into the feed."""
    g = {k: g.get(k) for k in GOVERNANCE_FIELDS}
    lsb = g.get("latestSignedBlock")
    try:
        lsb = int(lsb) if lsb not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        lsb = None
    dc = g.get("dataCenter") or {}
    out = {
        "name": ((g.get("name") or "").strip() or None) if CONFIG.get("include_node_names", False) else None,
        "status": g.get("status") if isinstance(g.get("status"), str) else None,
        "last_signed_block": lsb,
        "blocks_since_last_signed": max(0, head - lsb) if lsb else None,
    }
    if CONFIG.get("include_data_centers", True) and isinstance(dc, dict) and (dc.get("name") or dc.get("location")):
        out["data_center"] = {"name": str(dc.get("name") or "").strip() or None,
                              "location": str(dc.get("location") or "").strip() or None}
    return out


STATUS_ORDER = {"MASTERNODE": 0, "STANDBY": 1, "SLASHED": 2}


def nakamoto(counts):
    """Smallest number of owners holding more than 1/3 and more than 1/2 of the seats."""
    total = sum(counts)
    out = {}
    for label, frac in (("one_third", Decimal(1) / 3), ("one_half", Decimal(1) / 2)):
        run, k = 0, 0
        for c in sorted(counts, reverse=True):
            run += c
            k += 1
            if Decimal(run) > frac * total:
                break
        out[label] = k if total else None
    return out


def step_validators(log, network="mainnet"):
    net_cfg = CONFIG.get(network, {}) if network != "mainnet" else {}
    rpc = general_rpc(network)
    chain = Chain(rpc, network)
    try:
        head = chain.head()
        epoch = chain.epoch_of(head)
        switch = chain.switch_block(epoch, head)
        masternodes, slashed = chain.sets_at(switch)
        if not masternodes:
            raise RuntimeError(f"block {switch} lists no masternodes")
        raw = rpc.call("eth_call", [{"to": VALIDATOR_CONTRACT, "data": SEL_GET_CANDIDATES}, "latest"])
        # The contract blanks a candidate's slot when it resigns, so the list contains zero addresses.
        candidates = sorted((set(abi_addr_array(raw)) | set(masternodes) | set(slashed)) - {ZERO})
        caps = rpc.batch([("eth_call", [{"to": VALIDATOR_CONTRACT, "data": SEL_GET_CANDIDATE_CAP + abi_addr(c)}, "latest"])
                          for c in candidates])
        owner_of = owners_at(rpc, candidates)
        # Stake composition: the owner's own stake (the owner is the first voter of their candidate) and votes.
        live = [c for c, cap in zip(candidates, caps) if (to_int(cap) or 0) > 0 and owner_of.get(c)]
        own_caps = rpc.batch([("eth_call", [{"to": VALIDATOR_CONTRACT, "data": SEL_GET_VOTER_CAP + abi_addr(c) + abi_addr(owner_of[c])}, "latest"])
                              for c in live])
        voter_lists = rpc.batch([("eth_call", [{"to": VALIDATOR_CONTRACT, "data": SEL_GET_VOTERS + abi_addr(c)}, "latest"])
                                 for c in live])
        composition = {}
        for c, oc, vl in zip(live, own_caps, voter_lists):
            others = set(abi_addr_array(vl)) - {owner_of[c]}
            composition[c] = (to_int(oc) or 0, len(others))
        params = {}
        for key, sel in CONTRACT_PARAMS.items():
            try:
                params[key] = to_int(rpc.call("eth_call", [{"to": VALIDATOR_CONTRACT, "data": sel}, "latest"]))
            except Exception as e:  # noqa: BLE001
                log.append(f"Contract parameter {key} unavailable: {str(e)[:80]}")
    finally:
        chain.save()

    gov, gov_src = governance_candidates(log, net_cfg.get("governance_api") if network != "mainnet" else None)
    sla = (read_json(os.path.join(STATE, "sla.json")) or {"days": {}}) if network == "mainnet" else {"days": {}}
    last_day = max(sla["days"]) if sla["days"] else None
    windows = {d: (sla_window(sla, d, last_day) if last_day else {"epochs": 0, "nodes": {}}) for d in (7, 30)}
    mn, sl = set(masternodes), set(slashed)
    rows = []
    for c, cap_hex in zip(candidates, caps):
        cap = to_int(cap_hex) or 0
        status = "MASTERNODE" if c in mn else "SLASHED" if c in sl else "STANDBY" if cap > 0 else None
        if status is None:
            continue   # no stake and no seat: not a candidate any more
        row = {"candidate": c, "owner": owner_of.get(c), "status": status,
               "stake_wei": str(cap), "stake_xdc": wei_to_xdc(cap)}
        if c in composition:
            own, n_voters = composition[c]
            row["owner_stake_xdc"] = wei_to_xdc(own)
            row["votes_xdc"] = wei_to_xdc(max(0, cap - own))
            row["other_voters"] = n_voters
        for label, days in (("history_7d", 7), ("history_30d", 30)):
            w = windows[days]
            if w["epochs"]:
                mn_epochs, slashed_epochs = w["nodes"].get(c, [0, 0])
                row[label] = {"epochs_recorded": w["epochs"], "masternode_epochs": mn_epochs, "slashed_epochs": slashed_epochs}
        if c in gov:
            row["governance"] = governance_fields(gov[c], head)
            gap = row["governance"]["blocks_since_last_signed"]
            if status == "MASTERNODE" and gap is not None:
                row["not_signing_recently"] = gap > NOT_SIGNING_BLOCKS
        rows.append(row)
    rows.sort(key=lambda r: (STATUS_ORDER[r["status"]], -int(r["stake_wei"]), r["candidate"]))
    # If most masternodes look silent, the governance site's crawler has stalled, not the network:
    # drop the flags rather than report every node as stopped.
    flagged = [r for r in rows if "not_signing_recently" in r]
    if flagged and sum(r["not_signing_recently"] for r in flagged) > len(flagged) / 2:
        log.append("Governance site's last-signed data looks stale (most masternodes would be flagged); "
                   "not_signing_recently left out this run")
        for r in flagged:
            del r["not_signing_recently"]
        sig_stale = True
    else:
        sig_stale = False

    by_status = {}
    for r in rows:
        s = by_status.setdefault(r["status"], {"count": 0, "stake_wei": 0})
        s["count"] += 1
        s["stake_wei"] += int(r["stake_wei"])
    for s in by_status.values():
        s["stake_xdc"] = wei_to_xdc(s["stake_wei"])
        s["stake_wei"] = str(s["stake_wei"])
    total_stake = sum(int(r["stake_wei"]) for r in rows)

    owners = {}
    for r in rows:
        if not r["owner"]:
            continue
        o = owners.setdefault(r["owner"], {"owner": r["owner"], "nodes": 0, "masternodes": 0, "stake_wei": 0})
        o["nodes"] += 1
        o["masternodes"] += r["status"] == "MASTERNODE"
        o["stake_wei"] += int(r["stake_wei"])
    top = sorted(owners.values(), key=lambda o: (-o["masternodes"], -o["nodes"], o["owner"]))[:25]
    for o in top:
        o["stake_xdc"] = wei_to_xdc(o["stake_wei"])
        o["stake_wei"] = str(o["stake_wei"])
    mn_counts = [o["masternodes"] for o in owners.values() if o["masternodes"]]

    summary = {
        "block_number": head,
        "epoch": epoch,
        "epoch_switch_block": switch,
        "candidates": len(rows),
        "by_status": by_status,
        "total_stake_wei": str(total_stake),
        "total_stake_xdc": wei_to_xdc(total_stake),
        "unique_owners": len(owners),
        "masternode_owners": len(mn_counts),
        "owners_with_several_masternodes": sum(1 for c in mn_counts if c > 1),
        "parameters": {
            **{k: (str(v) if k.endswith("_wei") else v) for k, v in params.items()},
            **({"min_candidate_stake_xdc": wei_to_xdc(params["min_candidate_stake_wei"])} if "min_candidate_stake_wei" in params else {}),
            **({"min_voter_stake_xdc": wei_to_xdc(params["min_voter_stake_wei"])} if "min_voter_stake_wei" in params else {}),
        },
        "governance_api": gov_src,
        "masternodes_not_signing_recently": sum(1 for r in rows if r.get("not_signing_recently")) if gov_src and not sig_stale else None,
        "status_differs_from_governance": sum(1 for r in rows if r.get("governance") and r["governance"].get("status")
                                              and r["governance"]["status"] != r["status"]) if gov_src else None,
        "owner_concentration": {
            **nakamoto(mn_counts),
            "note": "Fewest owner addresses that together hold more than 1/3 (or 1/2) of masternode seats. "
                    "One organisation may use several owner addresses, so real concentration can be higher.",
        },
    }
    dataset = "validators" if network == "mainnet" else f"validators/{network}"
    path = os.path.join(V1, dataset, "latest.json")
    old = read_json(path)
    changed = not (old and old.get("candidates") == rows and old.get("summary", {}).get("epoch") == epoch)
    write_json(path, {**meta("validators.v1.json", 3600), "network": network, "chain_id": NETWORKS[network]["chain_id"],
                      "status_meaning": {
                          "MASTERNODE": "In the masternode set for the current epoch (from the epoch switch block header).",
                          "STANDBY": "A candidate with stake that is not in the current masternode set.",
                          "SLASHED": "Penalised at the current epoch switch (listed in the block header's penalties).",
                      },
                      "governance_note": (
                          "The governance object on each candidate comes from the XDC governance dApp API "
                          f"({gov_src or 'unavailable this run'}): the last block the node signed, as seen by that "
                          "site's crawler"
                          + (", the data centre registered there" if CONFIG.get("include_data_centers", True) else "")
                          + (" and the node's registered name" if CONFIG.get("include_node_names") else "")
                          + f". not_signing_recently means a MASTERNODE has not signed for more than {NOT_SIGNING_BLOCKS} "
                          "blocks (about an hour); it is left out when that site's data looks out of date. The governance "
                          "site numbers epochs by block number, which differs from the XDPoS 2.0 epoch numbers (by "
                          "consensus round) used everywhere in this feed."),
                      "history_note": (
                          "history_7d / history_30d on each candidate count epochs recorded by this feed in the last 7 / 30 "
                          "UTC days ending on the most recent recorded day: epochs it was an eligible masternode and epochs "
                          "it was slashed. They fill up as the feed runs, or after a backfill."),
                      "summary": summary, "top_owners": top, "candidates": rows})
    line = {"generated_at": now_iso(), **summary,
            "owner_concentration": {k: v for k, v in summary["owner_concentration"].items() if k != "note"}}
    append_history(dataset, line["generated_at"][:10], line, "generated_at")
    return (("Updated" if changed else "Checked, no change") + f" ({len(rows)} candidates, {len(owners)} owners"
            + (", with governance data)" if gov_src else ", governance API unavailable)"))


# --------------------------------------------------------------------------
# Realised yield: what masternodes actually earned, from measured rewards
# --------------------------------------------------------------------------

def _epoch_seconds(points):
    """Average length of one epoch in seconds, from the first and last recorded (epoch number, time) pairs.
    Using epoch numbers means gaps in the record do not stretch the figure. None with fewer than two epochs."""
    pts = sorted((int(e), datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()) for e, t in points)
    if len(pts) < 2 or pts[-1][0] == pts[0][0]:
        return None
    return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0])


def step_yield(log):
    sla = read_json(os.path.join(STATE, "sla.json")) or {"days": {}}
    if not sla["days"]:
        return "Skipped: no epochs recorded yet"
    vals = read_json(os.path.join(V1, "validators", "latest.json")) or {}
    params = (vals.get("summary") or {}).get("parameters") or {}
    min_stake = to_int(params.get("min_candidate_stake_wei")) if params.get("min_candidate_stake_wei") else None
    own_stake, own_mn = {}, {}
    for c in vals.get("candidates") or []:
        if c.get("status") == "MASTERNODE" and c.get("owner") and c.get("owner_stake_xdc") is not None:
            own_stake[c["owner"]] = own_stake.get(c["owner"], 0) + Decimal(str(c["owner_stake_xdc"]))
            own_mn[c["owner"]] = own_mn.get(c["owner"], 0) + 1
    price = current_price()
    last_day = max(sla["days"])
    # One epoch length for every window, from the widest one: epochs are a steady 900 rounds.
    epoch_s = _epoch_seconds(sla_window(sla, SLA_KEEP_DAYS, last_day)["points"])

    network, owners = {}, {}
    for label, days in (("1d", 1), ("7d", 7), ("30d", 30)):
        w = sla_window(sla, days, last_day)
        typ = [int(t) for t in w["typical"] if t]
        entry = {"epochs": w["epochs"], "epochs_with_typical": len(typ),
                 "days_covered": round(w["epochs"] * epoch_s / 86400, 2) if epoch_s else None,
                 "complete": bool(epoch_s and w["epochs"] >= 0.9 * days * 86400 / epoch_s)}
        if typ and epoch_s:
            # Average typical reward per epoch x epochs per day, so epochs missing from the record don't drag the rate down.
            per_epoch = sum(typ) // len(typ)
            per_day = Decimal(per_epoch) * Decimal(86400) / Decimal(epoch_s) / Decimal(WEI)
            entry["typical_node_reward_per_epoch_xdc"] = wei_to_xdc(per_epoch)
            entry["typical_node_xdc_per_day"] = round(float(per_day), 4)
            entry["typical_node_xdc_per_year"] = round(float(per_day * 365), 2)
            if min_stake:
                entry["apr_pct_on_min_stake"] = round(float(per_day * 365 / (Decimal(min_stake) / WEI) * 100), 3)
            if price and price.get("usd") is not None:
                entry["typical_node_usd_per_day"] = round(float(per_day) * price["usd"], 2)
        network[label] = entry

        for o, v in w["owners"].items():
            row = owners.setdefault(o, {"owner": o, "masternodes_now": own_mn.get(o, 0),
                                        "owner_stake_xdc": float(own_stake[o]) if o in own_stake else None})
            r = {"epochs": v["epochs"], "reward_xdc": wei_to_xdc(v["reward_wei"]),
                 "avg_vs_typical": round(v["vs_sum"] / v["vs_n"], 4) if v["vs_n"] else None,
                 "shortfall_nodes": round(v["shortfall_nodes"], 2), "zero_reward_epochs": v["zero"]}
            if epoch_s and o in own_stake and own_stake[o] > 0 and v["epochs"] and v["node_epochs"]:
                # Rewards per day over the epochs the owner had eligible nodes, annualised, divided by the stake
                # behind them: the owner's own stake per current masternode x the average number of nodes they
                # had per epoch (so adding or losing a node part-way through doesn't skew it).
                per_node = own_stake[o] / own_mn[o]
                avg_nodes = Decimal(v["node_epochs"]) / Decimal(v["epochs"])
                days_present = Decimal(v["epochs"]) * Decimal(epoch_s) / Decimal(86400)
                r["apr_pct"] = round(float(Decimal(v["reward_wei"]) / WEI / days_present * 365
                                           / (per_node * avg_nodes) * 100), 3)
                r["avg_masternodes"] = round(float(avg_nodes), 2)
            row[label] = r

    rows = sorted(owners.values(), key=lambda r: (-(r.get("30d") or {}).get("reward_xdc", 0), r["owner"]))
    doc = {
        **meta("yield.v1.json", 3600),
        "network": "mainnet",
        "chain_id": 50,
        "as_of_day": last_day,
        "price": price,
        "min_candidate_stake_xdc": wei_to_xdc(min_stake) if min_stake else None,
        "method_note": (
            "Information only; not financial or investment advice. Past rewards do not predict future rewards. "
            "Realised, not modelled: built from the measured rewards in epoch-rewards. The typical node figures use "
            "the median single-node owner reward per epoch. Rates are simple annualised (no compounding) and only as "
            "good as the window's coverage: check complete and days_covered. Windows are whole UTC days ending with "
            "the most recent recorded day, which may be only partly over (so 1d is often incomplete). Owner APR is the "
            "owner's measured rewards over the epochs they had eligible nodes, annualised, divided by their own stake "
            "per masternode x the average number of masternodes they had (votes from others are excluded, as voters "
            "receive 0 %)."),
        "typical_masternode": network,
        "owners": rows,
    }
    path = os.path.join(V1, "yield", "latest.json")
    write_json(path, doc)   # always: generated_at must show the data is current
    t30 = network["30d"]
    if t30.get("typical_node_xdc_per_day") is None:
        return "Updated (no daily rate yet: that needs at least two recorded epochs)"
    return f"Updated ({t30['typical_node_xdc_per_day']} XDC/day per typical node over {t30.get('days_covered')} days)"


# --------------------------------------------------------------------------
# Events and alerts
# --------------------------------------------------------------------------
# Events are changes worth telling someone about. They are worked out by comparing this run with the
# last one (state/events.json), so nothing is announced on the first run, and each change is announced once.

EVENTS_KEEP = 300
LOW_PERFORMANCE = 0.9   # an owner below 90 % of node_count x typical reward


def _event(etype, severity, title, detail, network="mainnet", **ids):
    ev = {"time": now_iso(), "type": etype, "severity": severity, "network": network,
          "title": title, "detail": detail, **{k: v for k, v in ids.items() if v is not None}}
    ev["id"] = hashlib.sha256(json.dumps({k: ev[k] for k in ev if k != "time"}, sort_keys=True).encode()
                              + ev["time"][:13].encode()).hexdigest()[:16]
    return ev


def _short(a):
    return "xdc" + a[2:6] + "…" + a[-4:] if a else "?"


def detect_events(prev, log):
    """Compare the current published data with the previous run's snapshot. Returns (events, new_snapshot)."""
    events, snap = [], {"nodes": {}, "owners": dict(prev.get("owners") or {}), "rpcs": {}}
    first = not prev
    rewards_baseline = not prev.get("owners")   # no owner has been seen yet: record, announce nothing

    # Validators: status changes and signing gaps (both networks)
    for network, dataset in (("mainnet", "validators"), ("apothem", "validators/apothem")):
        vals = read_json(os.path.join(V1, dataset, "latest.json")) or {}
        before = {k: v for k, v in (prev.get("nodes") or {}).items() if k.startswith(network + ":")}
        if not vals.get("candidates"):
            snap["nodes"].update(before)   # nothing published for this network yet: keep what we knew
            continue
        baseline = first or not before     # first time this network is seen: record it, announce nothing
        for c in vals["candidates"]:
            key = f"{network}:{c['candidate']}"
            old = before.get(key)
            # No governance data this run (site down, or not a masternode): keep the last known signing state,
            # so an outage of the governance site is never reported as nodes stopping or resuming.
            known = c.get("not_signing_recently") is not None
            silent = bool(c["not_signing_recently"]) if known else bool((old or {}).get("silent")) and c["status"] == "MASTERNODE"
            cur = {"status": c["status"], "silent": silent, "owner": c.get("owner")}
            snap["nodes"][key] = cur
            name = (c.get("governance") or {}).get("name")
            label = f"{name} ({_short(c['candidate'])})" if name else _short(c["candidate"])
            ids = {"candidate": c["candidate"], "owner": c.get("owner"), "node_name": name}
            if baseline:
                continue
            if old is None:
                events.append(_event("candidate_added", "info", f"New candidate {label}",
                                     f"Proposed with {c.get('stake_xdc')} XDC staked; status {c['status']}.", network, **ids))
                continue
            if old["status"] != cur["status"]:
                sev = "critical" if cur["status"] == "SLASHED" else "warning" if old["status"] == "MASTERNODE" else "info"
                events.append(_event(f"status_{cur['status'].lower()}", sev, f"{label}: {old['status']} → {cur['status']}",
                                     f"Status changed at the current epoch switch on {network}.", network, **ids))
            if known and cur["silent"] and not old.get("silent"):
                g = (c.get("governance") or {})
                events.append(_event("stopped_signing", "critical", f"{label} has stopped signing",
                                     f"No signature for {g.get('blocks_since_last_signed')} blocks (about "
                                     f"{round((g.get('blocks_since_last_signed') or 0) * 2 / 60)} min), per the governance site.",
                                     network, **ids))
            if known and old.get("silent") and not cur["silent"] and cur["status"] == "MASTERNODE":
                events.append(_event("resumed_signing", "info", f"{label} is signing again", "Signing activity seen again.", network, **ids))
        if not baseline:
            for key, old in before.items():
                if key not in snap["nodes"]:
                    cand = key.split(":", 1)[1]
                    events.append(_event("candidate_removed", "warning", f"{_short(cand)} is no longer a candidate",
                                         f"It was {old['status']}; it has resigned or its stake was withdrawn.", network,
                                         candidate=cand, owner=old.get("owner")))

    # Rewards: owners that fall below (or recover to) the typical reward, or receive nothing
    for doc in RUN_EPOCHS:
        for r in doc["owners"]:
            o = r["owner"]
            if r.get("unverified") or int(r["reward_wei"]) < 0:
                continue   # a figure that could not be explained: say nothing rather than guess
            zero = int(r["reward_wei"]) == 0
            low = r.get("vs_typical") is not None and r["vs_typical"] < LOW_PERFORMANCE
            old = snap["owners"].get(o) or {}
            snap["owners"][o] = {"zero": zero, "low": low}
            if not old and rewards_baseline:
                continue
            ids = {"owner": o, "epoch": doc["paid_in_epoch"]}
            if zero and not old.get("zero"):
                events.append(_event("owner_unpaid", "critical", f"Owner {_short(o)} received no reward",
                                     f"Epoch {doc['paid_in_epoch']} (for epoch {doc['rewarded_epoch']}): 0 XDC across "
                                     f"{r['node_count']} masternode(s).", **ids))
            elif low and not zero and not old.get("low"):
                events.append(_event("owner_below_typical", "warning", f"Owner {_short(o)} earned {round(r['vs_typical'] * 100)} % of typical",
                                     f"Epoch {doc['paid_in_epoch']}: {r['reward_xdc']} XDC vs {r.get('expected_xdc')} expected "
                                     f"for {r['node_count']} node(s); about {r.get('shortfall_nodes')} node(s)' worth missing.", **ids))
            elif not low and not zero and (old.get("low") or old.get("zero")):
                events.append(_event("owner_recovered", "info", f"Owner {_short(o)} is back to the typical reward",
                                     f"Epoch {doc['paid_in_epoch']}: {r['reward_xdc']} XDC.", **ids))

    # RPC endpoints (from the hourly copy of the live feed)
    rh = read_json(os.path.join(V1, "rpc-health", "hourly.json")) or {}
    for net in rh.get("networks") or []:
        for e in net.get("endpoints") or []:
            key = f"{net['network']}:{e['url']}"
            down = e.get("status") == "down"
            snap["rpcs"][key] = down
            old = (prev.get("rpcs") or {}).get(key)
            if first or old is None:
                continue
            if down and not old:
                events.append(_event("rpc_down", "warning", f"RPC down: {e['url']}", e.get("error") or "No valid answer.",
                                     net["network"], endpoint=e["url"]))
            if old and not down:
                events.append(_event("rpc_up", "info", f"RPC back: {e['url']}", f"Answering again ({e.get('status')}).",
                                     net["network"], endpoint=e["url"]))
    if first:
        log.append("First run: recorded a baseline; events start from the next run")
    return events, snap


FEED_START = "2026-01-01T00:00:00Z"   # "updated" time of a feed that has no entries yet


def _atom_entries(events):
    """Entries carry their text as content, not as links: these feeds never ask a reader to open anything."""
    from xml.sax.saxutils import escape, quoteattr
    out = []
    for e in events:
        out += ["  <entry>", f"    <title>[{escape(e['severity'])}] {escape(e['title'])}</title>",
                f"    <id>urn:xdc-open-data:event:{escape(e['id'])}</id>", f"    <updated>{escape(e['time'])}</updated>",
                f"    <category term={quoteattr(e['type'])}/>",
                f"    <content type=\"text\">{escape(e.get('detail') or e['title'])}</content>", "  </entry>"]
    return out


def atom_feed(events):
    site = CONFIG["site_url"]
    updated = events[0]["time"] if events else FEED_START
    out = ['<?xml version="1.0" encoding="utf-8"?>', '<feed xmlns="http://www.w3.org/2005/Atom">',
           "  <title>XDC Open Data: network events</title>",
           f'  <link href="{site}v1/events/feed.xml" rel="self"/>', f'  <link href="{site}"/>',
           f"  <id>{site}v1/events/</id>", f"  <updated>{updated}</updated>",
           "  <author><name>XDC Outpost</name></author>"]
    out += _atom_entries(events[:50])
    out.append("</feed>")
    return "\n".join(out) + "\n"


OWNER_FEED_KEEP = 50


def owner_atom_feed(owner, events):
    """Atom feed of one owner's events. Hosted on GitHub Pages, so only this repo can write to it."""
    site = CONFIG["site_url"]
    url = f"{site}v1/events/owners/{owner}.xml"
    updated = events[0]["time"] if events else FEED_START
    out = ['<?xml version="1.0" encoding="utf-8"?>', '<feed xmlns="http://www.w3.org/2005/Atom">',
           f"  <title>XDC Open Data: events for owner {_short(owner)}</title>",
           f"  <subtitle>Status changes, signing gaps and reward problems for the masternodes of {owner} "
           "(xdc form: xdc" + owner[2:] + "). Information only.</subtitle>",
           f'  <link href="{url}" rel="self"/>', f'  <link href="{site}"/>',
           f"  <id>{url}</id>", f"  <updated>{updated}</updated>",
           "  <author><name>XDC Outpost</name></author>"]
    out += _atom_entries(events)
    out.append("</feed>")
    return "\n".join(out) + "\n"


def step_owner_feeds(new, log):
    """One Atom feed per owner at v1/events/owners/<owner>.xml (lowercase 0x address).
    Every current owner gets a feed (empty until something happens), so the URL works from day one.
    Feeds are only rewritten when that owner has a new event."""
    st_path = os.path.join(STATE, "owner-events.json")
    store = read_json(st_path) or {}
    folder = os.path.join(V1, "events", "owners")
    os.makedirs(folder, exist_ok=True)
    touched = set()
    is_addr = re.compile(r"0x[0-9a-f]{40}").fullmatch   # the address becomes a file name: accept nothing else
    for e in sorted(new, key=lambda e: e["time"]):
        o = e.get("owner")
        if not o or not is_addr(o) or e.get("network", "mainnet") != "mainnet":
            continue
        lst = store.setdefault(o, [])
        if e["id"] not in {x["id"] for x in lst}:
            lst.insert(0, e)
            del lst[OWNER_FEED_KEEP:]
            touched.add(o)
    vals = read_json(os.path.join(V1, "validators", "latest.json")) or {}
    owners = {o for o in {c.get("owner") for c in vals.get("candidates") or []} | set(store) if o and is_addr(o)}
    created = 0
    for o in sorted(owners):
        path = os.path.join(folder, f"{o}.xml")
        if o in touched or not os.path.exists(path):
            created += write_text_if_changed(path, owner_atom_feed(o, store.get(o, [])))
    write_json(st_path, store, compact=True)
    return len(touched), created


def _plain(e):
    """Alert text without node names or URLs: names are registered by anyone, and our alerts never contain links."""
    title, detail = e["title"], e["detail"]
    if e.get("node_name") and e.get("candidate"):
        title = title.replace(f"{e['node_name']} ({_short(e['candidate'])})", _short(e["candidate"]))
    if e.get("endpoint"):
        host = urllib.parse.urlsplit(e["endpoint"]).netloc or e["endpoint"]
        title = title.replace(e["endpoint"], host)
    return title, detail


def _ntfy_post(server, topic, payload, token):
    """One publish. Returns (ok, retry_after_seconds_or_None)."""
    req = urllib.request.Request(server, data=json.dumps({"topic": topic, **payload}).encode(),
                                 headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                                          "Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req, timeout=15).read()
        return True, None
    except urllib.error.HTTPError as e:
        if e.code == 429:
            try:
                wait = int(e.headers.get("Retry-After") or 3600)
            except ValueError:
                wait = 3600
            return False, max(60, wait)
        return False, None
    except Exception:  # noqa: BLE001
        return False, None


def send_alerts(events, log):
    """Optional push notifications to ONE ntfy topic that only this feed can publish to.
    On ntfy.sh anyone can post to an open topic, so a guessable topic could carry fake alerts. This therefore
    only sends with an access token (GitHub secret NTFY_TOKEN) for a topic reserved as read-only for everyone
    else (a paid ntfy.sh plan or your own ntfy server). Kept far inside rate limits:
       - ONE digest message per hourly run, however many events (at most 24 a day);
       - a hard budget of max_per_day messages in any rolling 24 hours;
       - on HTTP 429 nothing is sent again until the Retry-After time has passed.
    Messages contain no links and no node names. Events not pushed are still in the JSON and Atom feeds."""
    cfg = CONFIG.get("alerts") or {}
    if not cfg.get("enabled"):
        return 0
    token = os.environ.get("NTFY_TOKEN", "").strip()
    if not token or not cfg.get("network_topic"):
        log.append("Alerts are enabled but not sent: add the NTFY_TOKEN repository secret and a network_topic "
                   "reserved as read-only for everyone else")
        return 0
    if not events:
        return 0
    st_path = os.path.join(STATE, "alerts.json")
    st = read_json(st_path) or {"sent": [], "paused_until": 0}
    now = time.time()
    st["sent"] = [t for t in st.get("sent", []) if now - t < 86400]
    if now < st.get("paused_until", 0):
        log.append("Alerts paused after a rate-limit response from ntfy; events are in the feed")
        write_json(st_path, st)
        return 0
    if len(st["sent"]) >= int(cfg.get("max_per_day", 24)):
        log.append("Daily alert budget reached; events are in the feed")
        write_json(st_path, st)
        return 0
    nets = cfg.get("networks") or ["mainnet"]
    order = {"critical": 0, "warning": 1, "info": 2}
    shown = [e for e in events if e.get("network", "mainnet") in nets
             and e["severity"] in (cfg.get("severities") or ["critical", "warning"])]
    if not shown:
        return 0
    shown.sort(key=lambda e: order[e["severity"]])
    texts = [_plain(e) for e in shown]
    lines = [f"• {t}" for t, _ in texts[:12]]
    if len(shown) > 12:
        lines.append(f"…and {len(shown) - 12} more in the events feed")
    msg = {"title": texts[0][0] if len(shown) == 1 else f"XDC: {len(shown)} new events",
           "message": texts[0][1] if len(shown) == 1 else "\n".join(lines),
           "priority": {"critical": 4, "warning": 3}.get(shown[0]["severity"], 2), "tags": ["xdc"]}
    ok, retry = _ntfy_post((cfg.get("ntfy_server") or "https://ntfy.sh").rstrip("/"), cfg["network_topic"], msg, token)
    if retry:
        st["paused_until"] = now + retry
        log.append(f"ntfy rate limit hit; pausing alerts for {retry // 60} min")
    elif ok:
        st["sent"].append(now)
    else:
        log.append("Alert failed (check the topic name, server and NTFY_TOKEN)")
    write_json(st_path, st)
    return 1 if ok else 0


def step_events(log):
    st_path = os.path.join(STATE, "events.json")
    prev = read_json(st_path) or {}
    events, snap = detect_events(prev, log)
    folder = os.path.join(V1, "events")
    latest = read_json(os.path.join(folder, "latest.json")) or {"events": []}
    seen = {e["id"] for e in latest["events"]}
    new = [e for e in events if e["id"] not in seen]
    for e in new:
        append_history("events", e["time"][:10], e, "id")
    if not os.path.exists(os.path.join(folder, "history", "index.json")):
        os.makedirs(os.path.join(folder, "history"), exist_ok=True)
        update_history_index("events")   # the catalog links to it, so it must exist before the first event
    all_events = (new[::-1] + latest["events"])[:EVENTS_KEEP]
    # Always rewrite latest.json so generated_at shows the check ran; feed.xml only changes with new events.
    write_json(os.path.join(folder, "latest.json"), {
        **meta("events.v1.json", 3600),
        "note": "Most recent first. Each change is reported once, when this feed first sees it (checked hourly). "
                "Atom feed of everything: feed.xml. One owner's Mainnet events: owners/<owner address, lowercase 0x>.xml."
                + (" Push alerts: see alerts in the catalog (v1/index.json)." if _alerts_on() else ""),
        "events": all_events})
    write_text_if_changed(os.path.join(folder, "feed.xml"), atom_feed(all_events))
    touched, created = step_owner_feeds(new, log)
    # Only now remember this run: if anything above failed, the same changes are found again next time.
    write_json(st_path, snap, compact=True)
    sent = send_alerts(new, log)
    return (f"{len(new)} new event(s), {touched} owner feed(s) updated" + (f", {created} feed file(s) written" if created else "")
            + (f", {sent} alert sent" if sent else ""))


def _alerts_on():
    """Alerts are only advertised and sent when switched on AND a publishing token is present."""
    return bool((CONFIG.get("alerts") or {}).get("enabled")) and bool(os.environ.get("NTFY_TOKEN", "").strip())


# --------------------------------------------------------------------------
# Node client versions and network upgrades
# --------------------------------------------------------------------------

STATS_SOURCES = {
    "mainnet": {"rest": ["https://stats1.xinfin.network", "https://stats.xinfin.network"],
                "ws": ["wss://stats1.xinfin.network/primus", "wss://xinfin.network/primus"]},
    "apothem": {"rest": ["https://api.apothem.network", "https://stats.apothem.network"], "ws": []},
}
RELEASES_API = "https://api.github.com/repos/XinFinOrg/XDPoSChain/releases/latest"
RAW_CONFIG = "https://raw.githubusercontent.com/XinFinOrg/XDPoSChain/{ref}/params/config_networks.go"
CONFIG_SECTION = {"mainnet": "XDCMainnetChainConfig", "apothem": "TestnetChainConfig"}


def _stats_nodes(network, log):
    src = STATS_SOURCES[network]
    for base in src["rest"]:
        try:
            d = fetch_json(base + "/v2/snapshot", attempts=1)
            if isinstance(d, dict) and isinstance(d.get("nodes"), list):
                return d["nodes"], base
        except Exception as e:  # noqa: BLE001
            log.append(f"{network} stats {base}: {str(e)[:60]}")
    try:
        import websocket  # websocket-client; installed by the workflow
    except ImportError:
        return None, None
    for url in src["ws"]:
        ws = None
        try:
            ws = websocket.create_connection(f"{url}?_primuscb={int(time.time())}", timeout=15,
                                             header=[f"User-Agent: {USER_AGENT}"], origin="https://xdcoutpost.xyz")
            ws.send(json.dumps({"emit": ["ready"]}))
            deadline = time.time() + 20
            while time.time() < deadline:
                m = json.loads(ws.recv())
                if isinstance(m, str):
                    if m.startswith("primus::ping::"):
                        ws.send(json.dumps("primus::pong::" + m[14:]))
                    continue
                nodes = None
                if isinstance(m, dict) and isinstance(m.get("emit"), list) and m["emit"][:1] == ["init"]:
                    nodes = (m["emit"][1] or {}).get("nodes") if len(m["emit"]) > 1 else None
                elif isinstance(m, dict) and m.get("action") == "init":
                    nodes = m["data"] if isinstance(m.get("data"), list) else (m.get("data") or {}).get("nodes")
                if isinstance(nodes, list):
                    return nodes, url
        except Exception as e:  # noqa: BLE001
            log.append(f"{network} stats {url}: {str(e)[:60]}")
        finally:
            try:
                ws and ws.close()
            except Exception:  # noqa: BLE001
                pass
    return None, None


def _client_version(s):
    """'XDC/v2.6.0-stable-a1b2c3d4/linux-amd64/go1.21.3' -> 'v2.6.0-stable'."""
    parts = str(s or "").split("/")
    v = parts[1] if len(parts) > 1 else ""
    if v and "-" in v and all(c in "0123456789abcdef" for c in v.rsplit("-", 1)[-1].lower()) and len(v.rsplit("-", 1)[-1]) >= 7:
        v = v.rsplit("-", 1)[0]
    return v or "unknown"


def _semver(v):
    import re
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", str(v))
    return tuple(int(x) for x in m.groups()) if m else None


def _parse_forks(text, section):
    """{ForkNameBlock: block or None} from one network's ChainConfig in params/config_networks.go.
    None means not scheduled (nil) or set from a named constant."""
    import re
    if not text:
        return None
    start = text.find(f"{section} = &ChainConfig{{")
    if start < 0:
        return None
    end = text.find("ChainConfig{", start + len(section) + 20)
    body = text[start:end if end > 0 else len(text)]
    forks = {}
    for name, val in re.findall(r"(\w+Block):\s*([^,\n]+),", body):
        num = re.fullmatch(r"big\.NewInt\((\d+)\)", val.strip())
        forks[name] = int(num.group(1)) if num else None
    return forks


def step_clients(log):
    out = {}
    release = None
    try:
        gh_token = os.environ.get("GITHUB_TOKEN")   # only raises GitHub's rate limit for this one public request
        req = urllib.request.Request(RELEASES_API, headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json",
                                                            **({"Authorization": f"Bearer {gh_token}"} if gh_token else {})})
        with urllib.request.urlopen(req, timeout=20) as r:
            rel = json.loads(r.read().decode())
        release = {"tag": rel.get("tag_name"), "name": rel.get("name"), "published_at": rel.get("published_at"),
                   "url": rel.get("html_url")}
    except Exception as e:  # noqa: BLE001
        log.append(f"Latest release unavailable: {str(e)[:80]}")

    cfg_text = {}
    for ref in ([release["tag"]] if release and release.get("tag") else []) + ["master"]:
        try:
            with urllib.request.urlopen(urllib.request.Request(RAW_CONFIG.format(ref=ref), headers={"User-Agent": USER_AGENT}),
                                        timeout=20) as r:
                cfg_text[ref] = r.read().decode()
        except Exception as e:  # noqa: BLE001
            log.append(f"Network config at {ref} unavailable: {str(e)[:60]}")

    for network in ("mainnet", "apothem"):
        entry = {"network": network, "chain_id": NETWORKS[network]["chain_id"]}
        # versions reported to the public stats server
        nodes, src = _stats_nodes(network, log)
        if nodes is not None:
            versions, mn_versions = {}, {}
            dataset = "validators" if network == "mainnet" else f"validators/{network}"
            vals = read_json(os.path.join(V1, dataset, "latest.json")) or {}
            masternodes = {c["candidate"] for c in vals.get("candidates") or [] if c.get("status") == "MASTERNODE"}
            active = 0
            for n in nodes:
                st, info = n.get("stats") or {}, n.get("info") or {}
                if not st.get("active"):
                    continue
                active += 1
                v = _client_version(info.get("node"))
                versions[v] = versions.get(v, 0) + 1
                cb = info.get("coinbase")
                if cb and addr0x(cb) in masternodes:
                    mn_versions[addr0x(cb)] = v
            order = sorted(versions, key=lambda v: (_semver(v) or (0, 0, 0), v), reverse=True)
            entry.update({"stats_source": src, "nodes_reporting": active,
                          "versions": [{"version": v, "nodes": versions[v]} for v in order]})
            mn_counts = {}
            for v in mn_versions.values():
                mn_counts[v] = mn_counts.get(v, 0) + 1
            entry["masternodes"] = {"total": len(masternodes), "reporting": len(mn_versions),
                                    "versions": [{"version": v, "masternodes": mn_counts[v]}
                                                 for v in sorted(mn_counts, key=lambda v: (_semver(v) or (0, 0, 0), v), reverse=True)],
                                    "by_candidate": dict(sorted(mn_versions.items()))}
            if release and _semver(release["tag"]):
                latest = _semver(release["tag"])
                on_latest = sum(c for v, c in versions.items() if _semver(v) and _semver(v) >= latest)
                mn_latest = sum(1 for v in mn_versions.values() if _semver(v) and _semver(v) >= latest)
                entry["on_latest_release_pct"] = round(on_latest / active * 100, 1) if active else None
                entry["masternodes"]["on_latest_release_pct"] = round(mn_latest / len(mn_versions) * 100, 1) if mn_versions else None
        else:
            entry["stats_source"] = None

        # scheduled upgrades
        try:
            chain = Chain(general_rpc(network), network)
            head = chain.head()
            t_head = to_int(chain.header(head)["timestamp"])
            t_old = to_int(chain.header(max(1, head - 1000))["timestamp"])
            bt = (t_head - t_old) / 1000
            forks_rel = _parse_forks(cfg_text.get(release["tag"]), CONFIG_SECTION[network]) if release and release.get("tag") in cfg_text else None
            forks_dev = _parse_forks(cfg_text.get("master"), CONFIG_SECTION[network]) if "master" in cfg_text else None
            upcoming = []
            for name in sorted(set(forks_rel or {}) | set(forks_dev or {})):
                blk_r, blk_d = (forks_rel or {}).get(name), (forks_dev or {}).get(name)
                blk = blk_r if blk_r is not None else blk_d
                if blk is None or blk <= head:
                    continue
                upcoming.append({"fork": name, "block": blk, "blocks_to_go": blk - head,
                                 "eta": iso(t_head + (blk - head) * bt),
                                 "in_latest_release": blk_r is not None,
                                 "note": None if blk_r is not None else "Only in development code (master), not yet in a release"})
            entry["upgrades"] = {"head_block": head, "avg_block_time_seconds": round(bt, 3),
                                 "upcoming": sorted(upcoming, key=lambda u: u["block"]),
                                 "source": "params/config_networks.go in XinFinOrg/XDPoSChain (latest release and master)"}
        except Exception as e:  # noqa: BLE001
            log.append(f"{network} upgrades unavailable: {str(e)[:100]}")
        out[network] = entry

    doc = {**meta("clients.v1.json", 3600), "latest_release": release,
           "note": "Versions are what nodes report to the public XinFin stats server (only nodes that report are "
                   "counted). Masternode versions are matched by coinbase address. No node names, IPs or locations are "
                   "published. Upcoming upgrades are read from the network config in the client source code; an upgrade "
                   "that is only in master may change before release.",
           "networks": [out["mainnet"], out["apothem"]]}
    path = os.path.join(V1, "clients", "latest.json")
    write_json(path, doc)
    m = out["mainnet"]
    return f"Updated (mainnet: {m.get('nodes_reporting')} nodes reporting, {len((m.get('upgrades') or {}).get('upcoming') or [])} upcoming upgrade(s))"


# --------------------------------------------------------------------------
# Ecosystem projects (from XDC Atlas)
# --------------------------------------------------------------------------

def step_ecosystem(log):
    src = fetch_json(CONFIG["atlas_tools_url"])
    base = CONFIG["atlas_base_url"]
    projects = []
    for t in src.get("tools", []):
        p = {
            "id": t.get("id"),
            "name": t.get("name"),
            "summary": t.get("summary"),
            "category": t.get("category"),
            "type": t.get("type"),
            "kind": t.get("kind"),
            "status": t.get("status"),
            "license": t.get("license"),
            "tags": t.get("tags", []),
            "by": t.get("by"),
            "by_url": t.get("byUrl"),
            "links": t.get("links", {}),
            "claimed": bool(t.get("claimed", False)),
        }
        if t.get("icon"):
            p["icon_url"] = t["icon"] if t["icon"].startswith("http") else base + t["icon"].lstrip("./")
        if t.get("tipAddress"):
            p["tip_address"] = t["tipAddress"]
        projects.append(p)
    projects.sort(key=lambda p: (p["category"] or "", (p["name"] or "").lower()))
    categories = src.get("categories", {})

    path = os.path.join(V1, "ecosystem", "projects.json")
    old = read_json(path) or {}
    changed = old.get("projects") != projects or old.get("categories") != categories
    write_json(path, {
        **meta("ecosystem-projects.v1.json", 3600),
        "origin": base,
        "add_or_update": "Projects are listed through pull requests to https://github.com/s4njk4n/XDC_Atlas",
        "count": len(projects),
        "categories": categories,
        "projects": projects,
    })
    return ("Updated" if changed else "Checked, no change") + f" ({len(projects)} projects)"


# --------------------------------------------------------------------------
# Hourly archive of the live Worker feed
# --------------------------------------------------------------------------

DROP = {"$schema", "version", "docs", "license", "source", "update_interval_seconds", "stale_after_seconds"}


def step_archive(log):
    if not worker_configured():
        return "Skipped: set worker_url in config.json"
    base = CONFIG["worker_url"].rstrip("/")
    done = []
    for dataset in ("rpc-health", "network"):
        doc = fetch_json(f"{base}/v1/{dataset}/latest.json")
        if not doc.get("generated_at"):
            raise RuntimeError(f"{dataset}: no generated_at in live feed")
        hourly = dict(doc)
        hourly["$schema"] = SCHEMAS + f"{dataset}.v1.json"
        hourly["note"] = "Hourly copy of the live feed. Prefer the live URL; use this if it is unreachable."
        hourly["live_url"] = f"{base}/v1/{dataset}/latest.json"
        write_json(os.path.join(V1, dataset, "hourly.json"), hourly)

        line = {k: v for k, v in doc.items() if k not in DROP}
        added = append_history(dataset, doc["generated_at"][:10], line, "generated_at")
        done.append(f"{dataset} {'archived' if added else 'already archived'}")
    return ", ".join(done)


# --------------------------------------------------------------------------
# Catalog: v1/index.json and llms.txt
# --------------------------------------------------------------------------

def catalog():
    site, worker = CONFIG["site_url"], CONFIG["worker_url"].rstrip("/")
    live = worker_configured()
    ds = []

    def add(id_, title, desc, url, schema, interval, history=None, fallback=None, line_schema=None, extra=None):
        d = {"id": id_, "title": title, "description": desc, "url": url,
             "schema": SCHEMAS + schema, "update_interval_seconds": interval, "format": "application/json"}
        if history:
            d["history"] = {"index": history, "line_schema": SCHEMAS + (line_schema or schema),
                            "format": "application/x-ndjson", "one_file_per": "UTC day"}
        if fallback:
            d["fallback_url"] = fallback
        if extra:
            d.update(extra)
        ds.append(d)

    if live:
        add("rpc-health", "XDC RPC endpoint health",
            "Every public XDC RPC endpoint listed in XDC Atlas, checked every 5 minutes from Cloudflare's network: "
            "status (ok/degraded/down), block lag, latency, 24-hour uptime, and the recommended endpoints right now. "
            "Covers Mainnet (chain 50) and Apothem testnet (chain 51).",
            f"{worker}/v1/rpc-health/latest.json", "rpc-health.v1.json", 300,
            f"{site}v1/rpc-health/history/index.json", f"{site}v1/rpc-health/hourly.json")
        add("network", "XDC network snapshot",
            "Latest block, average block time, gas price, current epoch and round, and estimated time to the next "
            "epoch. Mainnet and Apothem.",
            f"{worker}/v1/network/latest.json", "network.v1.json", 300,
            f"{site}v1/network/history/index.json", f"{site}v1/network/hourly.json")
        add("feed-status", "Live feed status", "Whether the live feed's last run succeeded and how long ago it ran.",
            f"{worker}/v1/status.json", "status.v1.json", 300)
    add("epoch-rewards", "XDC epoch rewards by owner (Mainnet)",
        "Masternode rewards credited at the most recent epoch switch, per owner address. Rewards are not transfers: "
        "they are measured from each owner's balance change across the epoch switch block. An owner with several "
        "nodes gets one combined amount, so each row also gives the owner's node count, the typical reward of one "
        "fully working node (from single-node owners), what the owner should have received, any shortfall, and an "
        "equal-share estimate per node. A figure that cannot be fully explained is marked unverified and left out of "
        "totals. Rewards paid in epoch N are for epoch N-2.",
        f"{site}v1/epoch-rewards/latest.json", "epoch-rewards.v1.json", 3600,
        f"{site}v1/epoch-rewards/history/index.json", line_schema="epoch-rewards-summary.v1.json",
        extra={"daily_totals": {"index": f"{site}v1/epoch-rewards/daily/index.json",
                                "schema": SCHEMAS + "epoch-rewards-daily.v1.json",
                                "description": "Per-owner reward totals for each UTC day, for accounting and tax. "
                                               "Each day is also a CSV file (csv link in the index)."},
               "monthly_csv": {"index": f"{site}v1/epoch-rewards/monthly/index.json",
                               "description": "One CSV per month: a row per owner per day (date, owner, owner_xdc_form, "
                                              "reward_xdc, reward_wei, epochs_paid, node_count, unverified_epochs"
                                              + (", value_usd, epochs_valued" if _prices_on() else "") + ")."}})
    add("validators", "XDC validators and candidates (Mainnet)",
        "Every masternode candidate: owner address, stake and status (MASTERNODE, STANDBY or SLASHED), plus totals by status, the largest owners and owner concentration (how few owners hold 1/3 or 1/2 "
        "of masternode seats). Where the XDC governance dApp API is reachable, each candidate also has "
        + ("its registered name, " if CONFIG.get("include_node_names") else "")
        + "the last block it signed and a flag for masternodes that have stopped signing.",
        f"{site}v1/validators/latest.json", "validators.v1.json", 3600,
        f"{site}v1/validators/history/index.json", line_schema="validators-summary.v1.json")
    add("validators-apothem", "XDC validators and candidates (Apothem testnet)",
        "The same as the Mainnet validators dataset, for the Apothem testnet (chain 51), so teams can test against it.",
        f"{site}v1/validators/apothem/latest.json", "validators.v1.json", 3600,
        f"{site}v1/validators/apothem/history/index.json", line_schema="validators-summary.v1.json")
    add("events", "XDC network events",
        "Changes worth acting on, each reported once: masternode status changes (MASTERNODE, STANDBY, SLASHED), nodes "
        "that stop or resume signing, candidates added or removed, owners who receive no reward or fall below 90 % of "
        "the typical reward (and recover), and RPC endpoints going down or coming back. Also as an Atom feed of "
        "everything, and an Atom feed per owner address with just that owner's Mainnet events"
        + (", plus push alerts through ntfy (see alerts in the catalog)." if _alerts_on() else "."),
        f"{site}v1/events/latest.json", "events.v1.json", 3600,
        f"{site}v1/events/history/index.json", line_schema="event.v1.json",
        extra={"atom": f"{site}v1/events/feed.xml",
               "owner_feed": {"url_template": f"{site}v1/events/owners/{{owner}}.xml",
                              "description": "Atom feed of one owner's Mainnet events. {owner} is the owner address in "
                                             "lowercase 0x form. Exists for every current candidate owner. Only this "
                                             "feed can write to it."}})
    add("clients", "XDC node client versions and upcoming upgrades",
        "Client versions reported by nodes to the public XinFin stats server (all nodes and masternodes, share on the "
        "latest release, each masternode's version), the latest XDPoSChain release, and network upgrades scheduled by "
        "block number with estimated dates. Mainnet and Apothem.",
        f"{site}v1/clients/latest.json", "clients.v1.json", 3600)
    add("yield", "XDC masternode realised yield (Mainnet)",
        "What masternodes actually earned over the last 1, 7 and 30 days, from measured rewards: the typical "
        "fully working node's XDC per epoch, per day and per year, APR on the minimum stake, and for each owner "
        "their rewards, APR on their own stake, average performance against the typical node, and zero-reward epochs.",
        f"{site}v1/yield/latest.json", "yield.v1.json", 3600)
    add("ecosystem-projects", "XDC ecosystem projects",
        "Tools, apps and services built on XDC, curated in XDC Atlas: name, summary, category, status, licence and links.",
        f"{site}v1/ecosystem/projects.json", "ecosystem-projects.v1.json", 3600)

    return {
        "$schema": SCHEMAS + "catalog.v1.json",
        "name": "XDC Open Data",
        "description": "Free, open JSON about the XDC Network for AI agents, scripts and dashboards. No API key, no sign-up, CORS open.",
        "publisher": {"name": "XDC Outpost", "url": "https://xdcoutpost.xyz/"},
        "docs": site,
        "llms_txt": site + "llms.txt",
        "repository": CONFIG["repo_url"],
        "license": CONFIG["license"],
        "license_url": CONFIG["license_url"],
        "conventions": {
            "timestamps": "ISO 8601, UTC, ending in Z",
            "token_amounts": "Exact integer wei as strings (*_wei); *_xdc fields are rounded to 6 decimals for display",
            "addresses": "Lowercase 0x-prefixed hex. For the xdc form, replace the leading 0x with xdc",
            "freshness": "Every file has generated_at and update_interval_seconds; treat data older than stale_after_seconds as stale",
            "versioning": "Paths start with /v1/. Fields are only added within v1; breaking changes get /v2/",
        },
        "fair_use": "Poll no faster than each dataset's update_interval_seconds. Cache responses. No key needed.",
        "disclaimer": "Information only, provided as is without warranty. Not financial, tax or investment advice. Reward "
                      "figures are measured from public chain data and may contain errors; check important figures "
                      "against your own records.",
        "license_note": "CC BY 4.0 covers XDC Outpost's own work in this feed. Data that comes from third-party services "
                        "remains subject to their terms.",
        **({"alerts": {
            "ntfy_server": (CONFIG["alerts"].get("ntfy_server") or "https://ntfy.sh"),
            "network_topic": CONFIG["alerts"].get("network_topic"),
            "delivery": "One digest message per hourly run at most, for critical and warning events on Mainnet. "
                        "Only this feed can publish to the topic. Messages never contain links or ask you to do "
                        "anything. events is always the complete record.",
        }} if _alerts_on() else {}),
        "build_status": f"{site}v1/build-status.json",
        "changes": f"{site}v1/changes.json",
        "datasets": ds,
    }


def llms_txt(cat):
    lines = [
        "# XDC Open Data",
        "",
        "> Free, open JSON about the XDC Network (chain ID 50; Apothem testnet 51) for AI agents and scripts. "
        "No API key, no sign-up, CORS open. Published by XDC Outpost (xdcoutpost.xyz). Licence: " + cat["license"] + ". "
        "Information only; not financial, tax or investment advice.",
        "",
        "Start with the catalog, which lists every dataset with its URL, JSON Schema and update interval:",
        f"{cat['docs']}v1/index.json",
        "",
        "Conventions: timestamps are ISO 8601 UTC. Token amounts are exact integer wei as strings in *_wei fields "
        "(1 XDC = 10^18 wei); *_xdc fields are rounded for display. Addresses are lowercase 0x hex (swap 0x for xdc "
        "if a user expects the xdc form). Every file has generated_at and update_interval_seconds; data older than "
        "stale_after_seconds is stale. Please poll no faster than the update interval.",
        "",
        "## Datasets",
        "",
    ]
    for d in cat["datasets"]:
        mins = d["update_interval_seconds"] // 60
        lines.append(f"- [{d['title']}]({d['url']}): {d['description']} Updates every {mins} min. "
                     f"Schema: {d['schema']}"
                     + (f". Daily history: {d['history']['index']}" if d.get("history") else "")
                     + (f". Per-owner daily totals: {d['daily_totals']['index']}" if d.get("daily_totals") else "")
                     + (f". Monthly CSV: {d['monthly_csv']['index']}" if d.get("monthly_csv") else "")
                     + (f". Atom feed: {d['atom']}" if d.get("atom") else "")
                     + (f". Per-owner Atom feed: {d['owner_feed']['url_template']}" if d.get("owner_feed") else "")
                     + (f". Fallback copy: {d['fallback_url']}" if d.get("fallback_url") else ""))
    live = any(d["id"] == "rpc-health" for d in cat["datasets"])
    lines += [
        "",
        "## Answering common questions",
        "",
    ] + ([
        "- Which XDC RPC should I use right now? rpc-health: pick the network, use the first URL in `recommended`.",
        "- Is a given endpoint up? rpc-health `endpoints`: `status`, `lag_blocks`, `uptime_24h_pct`.",
        "- When is the next epoch? network `epoch.est_seconds_to_next_epoch` (an estimate; rounds can be skipped).",
    ] if live else []) + [
        "- Is an address a masternode, who owns it, and how much is staked? validators `candidates` (lowercase 0x form): "
        "`status`, `owner`, `stake_xdc`, `owner_stake_xdc`, `votes_xdc`.",
        "- Is a masternode online right now? validators `candidates`: `governance.blocks_since_last_signed`, `not_signing_recently`.",
        "- How reliable has a node been? validators `candidates`: `history_30d` (masternode and slashed epochs).",
        "- How decentralised are XDC validators? validators `summary.owner_concentration`.",
        "- How much did an owner earn last epoch? epoch-rewards `owners`: `reward_wei` (exact) or `reward_xdc`; one combined "
        "figure for all the owner's masternodes (`node_count`). If the row has `unverified: true`, the figure could not be "
        "fully explained (see its `note`): say so rather than quoting it as the reward.",
        "- How much did one node earn? Each owner row has `nodes`; a node's `basis` says what its figure is: `measured` "
        "or `exact` is that node's own reward, `equal_share_estimate` is the owner's total split evenly between their "
        "nodes, and `unverified` should not be quoted.",
        "- Is an owner's setup under-performing? epoch-rewards `owners`: `vs_typical` (1.0 = every node earned the typical "
        "reward), `shortfall_nodes`, `below_typical`.",
        "- What does an XDC masternode earn? yield `typical_masternode.30d`: `typical_node_xdc_per_day`, "
        "`apr_pct_on_min_stake` (check `complete`).",
        "- What has an owner earned over a period, at what rate? yield `owners` (`30d.reward_xdc`, `30d.apr_pct`); for "
        "specific dates, the epoch-rewards daily files or monthly CSV (epochs listed under `unverified` are not in the totals).",
        "- Did anything happen to a node? events: filter by `candidate` or `owner`. One owner's events as Atom: "
        f"{cat['docs']}v1/events/owners/<owner address, lowercase 0x>.xml."
        + (" Push alerts: see `alerts` in the catalog." if _alerts_on() else ""),
        "- Is a node on the latest client version? clients `networks[].masternodes.by_candidate`; upcoming upgrades in "
        "`networks[].upgrades.upcoming`.",
        "- Has the data format changed? v1/changes.json.",
        "",
        "## Optional",
        "",
        f"- [Docs]({cat['docs']}): human-readable guide with examples",
        f"- [Source code]({cat['repository']}): builder scripts, schemas and the Cloudflare Worker",
        "- [XDC Atlas](https://xdcoutpost.xyz/XDC_Atlas/): directory of XDC tools, guides and RPC endpoints",
        "- [XDC Network Stats](https://xdcoutpost.xyz/XDC_Network_Stats/): live network stats page",
        "- [Masternode Rewards Calculator](https://xdcoutpost.xyz/XDC_Masternode_Rewards_Calculator/): rewards for one owner over any period",
        "",
    ]
    return "\n".join(lines)


def step_catalog(log):
    cat = catalog()
    path = os.path.join(V1, "index.json")
    changed = read_json(path) != cat
    if changed:
        write_json(path, cat)
    changed |= write_text_if_changed(os.path.join(ROOT, "llms.txt"), llms_txt(cat))
    note = "" if worker_configured() else " (live datasets hidden until worker_url is set)"
    return ("Updated" if changed else "No change") + note


# --------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------

def diagnose():
    print("\n=== RPC capability check (Mainnet) ===")
    chain = Chain(general_rpc())
    head, switch, switch_hash = None, None, None
    try:
        head = chain.head()
        epoch = chain.epoch_of(head)
        switch = chain.switch_block(epoch, head)
        switch_hash = chain.header(switch)["hash"]
        mns, sl = chain.sets_at(switch)
        print(f"Head block {head}, epoch {epoch}, epoch started at block {switch}: {len(mns)} masternodes, {len(sl)} slashed")
    except Exception as e:  # noqa: BLE001
        print("Could not read epoch data from block headers:", e)

    def reads_past_state(one, url, back):
        """True only if balances at two different past blocks differ. The foundation wallet is paid at every
        epoch switch, so identical answers mean the endpoint ignored the block number."""
        a = one.call_one(url, "eth_getBalance", [FOUNDATION_WALLET, hex(head - back)])
        b = one.call_one(url, "eth_getBalance", [FOUNDATION_WALLET, hex(head - back - 2000)])
        if a is None or b is None:
            return False
        if to_int(a) == to_int(b):
            raise RuntimeError("same answer for different blocks")
        return True

    for url in list(dict.fromkeys(CONFIG["mainnet_rpcs"] + archive_urls())):
        one = RPC([url])
        row = []
        tests = [
            ("block", lambda: one.call_one(url, "eth_blockNumber", []) is not None),
            ("header_fields", lambda: bool(addresses_from_bytes((one.call_one(url, "eth_getBlockByNumber", [hex(switch), False]) or {}).get("validators"))) if switch else None),
            ("archive(-2000 blocks)", lambda: reads_past_state(one, url, 2000) if head else None),
            ("archive(-30 days)", lambda: reads_past_state(one, url, 1_296_000) if head else None),
            ("batch", lambda: isinstance(RPC.post(url, [{"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber", "params": []},
                                                        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}]), list)),
            ("reward_files", lambda: bool(one.call_one(url, "eth_getRewardByHash", [switch_hash])) if switch_hash else None),
            ("call_traces", lambda: isinstance(one.call_one(url, "debug_traceBlockByNumber", [hex(switch), {"tracer": "callTracer"}]), list) if switch else None),
        ]
        for label, fn in tests:
            try:
                ok = fn()
                row.append(f"{label}={'skipped' if ok is None else 'yes' if ok else 'no'}")
            except Exception as e:  # noqa: BLE001
                row.append(f"{label}=no ({str(e)[:50]})")
        print(f"{rpc_label(url)}\n    " + "  ".join(row))
    print("\n=== Governance dApp API check ===")
    for base in CONFIG.get("governance_api") or []:
        base = base.rstrip("/")
        try:
            j = fetch_json(f"{base}/candidates?limit=1&page=1")
            item = (j.get("items") or [{}])[0]
            print(f"{base}: candidate list ok ({j.get('total')} candidates; latestSignedBlock "
                  f"{'present' if item.get('latestSignedBlock') else 'missing'})")
        except Exception as e:  # noqa: BLE001
            print(f"{base}: unavailable ({str(e)[:80]})")
    print()
    print("Needed: header_fields=yes on a mainnet_rpcs entry, and archive(-2000 blocks)=yes on an archive_rpcs entry.\n"
          "batch=yes makes runs faster. reward_files=yes is a bonus (exact protocol figures). call_traces=yes on any\n"
          "archive endpoint lets XDC paid to an owner by a contract in an epoch's first block be excluded exactly\n"
          "(stake withdrawals are excluded exactly either way). No XDPoS_* methods are used.\n")


# --------------------------------------------------------------------------

def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    started = time.time()
    if SECRETS and not isinstance(sys.stdout, _ScrubStream):
        sys.stdout, sys.stderr = _ScrubStream(sys.stdout), _ScrubStream(sys.stderr)
    backfill = 0
    if "--backfill" in argv:
        try:
            backfill = max(0, min(200, int(argv[argv.index("--backfill") + 1])))
        except (IndexError, ValueError):
            backfill = 0
    RUN_EPOCHS.clear()
    _NO_REWARD_FILES.clear()
    _NO_TRACE.clear()
    RPC.strikes.clear()
    _RUN["deadline"] = time.monotonic() + RUN_BUDGET_SECONDS
    if "--diagnose" in argv:
        try:
            diagnose()
        except Exception as e:  # noqa: BLE001  (diagnostics must never stop the build)
            print("Diagnostics stopped early:", e)
    status = {"$schema": SCHEMAS + "build-status.v1.json", "last_build_at": now_iso(), "steps": {}}
    steps = [("epoch_rewards", lambda log: step_rewards(log, backfill)), ("validators", step_validators),
             ("validators_apothem", lambda log: step_validators(log, "apothem")),
             ("yield", step_yield),
             ("clients", step_clients),
             ("ecosystem", step_ecosystem), ("archive", step_archive), ("events", step_events), ("catalog", step_catalog)]
    for name, fn in steps:
        log = []
        try:
            result = fn(log)
            status["steps"][name] = {"ok": True, "result": scrub(str(result))}
            print(f"[ok]   {name}: {result}")
        except Exception as e:  # noqa: BLE001  (one failed step must not stop the others)
            status["steps"][name] = {"ok": False, "error": scrub(str(e))[:500]}
            print(f"[FAIL] {name}: {e}")
        status["steps"][name]["notes"] = [scrub(n) for n in dict.fromkeys(log)]
        for n in status["steps"][name]["notes"]:
            print(f"       note: {n}")
    status["ok"] = all(s["ok"] for s in status["steps"].values())
    write_json(os.path.join(V1, "build-status.json"), status)
    cleaned = scrub_tree(started)
    if cleaned:
        print(f"Removed a private RPC address from {cleaned} file(s) before publishing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
