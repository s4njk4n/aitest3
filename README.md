# XDC Open Data

Free, open JSON about the XDC Network for AI agents, scripts and dashboards. No API key, no sign-up, CORS open. Part of [XDC Outpost](https://xdcoutpost.xyz/).

- **Docs:** https://xdcoutpost.xyz/XDC_Open_Data/
- **Catalog (start here):** https://xdcoutpost.xyz/XDC_Open_Data/v1/index.json
- **For LLMs:** https://xdcoutpost.xyz/XDC_Open_Data/llms.txt
- **For AI assistants:** the [XDC Open Data MCP server](https://github.com/s4njk4n/XDC_Open_Data_MCP)

## Datasets

| Dataset | What's in it | Refresh |
|---|---|---|
| RPC health | Status, block lag, latency and 24-hour uptime of every public XDC RPC endpoint, and the best ones right now | 5 min |
| Network snapshot | Latest block, block time, gas price, epoch progress | 5 min |
| Epoch rewards | Rewards credited to each masternode owner every epoch, compared with the typical reward of one fully working node; daily and monthly totals as JSON and CSV | Hourly |
| Validators | Every candidate with owner, stake (own vs votes), status (MASTERNODE / STANDBY / SLASHED), last signed block, 7- and 30-day history; owner concentration | Hourly |
| Validators (Apothem) | The same for the Apothem testnet | Hourly |
| Realised yield | What a typical masternode and each owner actually earned over 1, 7 and 30 days, with APR | Hourly |
| Events | Slashing, status changes, nodes that stop or resume signing, unpaid owners, RPC outages; also as an [Atom feed](https://xdcoutpost.xyz/XDC_Open_Data/v1/events/feed.xml), plus one Atom feed per owner address (`v1/events/owners/<owner 0x address>.xml`) | Hourly |
| Client versions | Client versions in use, share on the latest release, each masternode's version, upcoming network upgrades | Hourly |
| Ecosystem projects | XDC tools and apps listed in [XDC Atlas](https://xdcoutpost.xyz/XDC_Atlas/) | Hourly |

Every dataset has a JSON Schema in [`v1/schemas/`](v1/schemas/), and most keep daily history as NDJSON (`v1/<dataset>/history/`). Format changes are listed in [`v1/changes.json`](v1/changes.json).

## Quick start

```bash
# A healthy XDC Mainnet RPC endpoint right now
curl -s "$(curl -s https://xdcoutpost.xyz/XDC_Open_Data/v1/index.json | jq -r '.datasets[] | select(.id=="rpc-health") | .url')" \
  | jq -r '.networks[] | select(.network=="mainnet") | .recommended[0]'
```

## Conventions

- **Timestamps:** ISO 8601, UTC.
- **Amounts:** exact integer wei as strings in `*_wei` fields (1 XDC = 10¹⁸ wei). `*_xdc` values are rounded for display.
- **Addresses:** lowercase `0x…`. Swap `0x` for `xdc` for the XDC form.
- **Freshness:** every file has `generated_at`, `update_interval_seconds` and `stale_after_seconds`. Treat older data as stale.
- **Versioning:** `/v1/` only gains fields; breaking changes get `/v2/`.
- **Fair use:** poll no faster than each dataset's update interval, and cache what you fetch.

## How epoch rewards are measured

XDC rewards aren't transfers. The protocol adds them to balances inside the first block of each epoch (block N): 90 % of each node's reward to its owner, 0 % to voters, 10 % to the foundation. So for each owner of a masternode that was eligible in the rewarded epoch:

```
reward = balance(N) − balance(N−1) − (the owner's own transactions in block N)
         − (block N's transaction fees, if one of their masternodes produced the block)
         − (XDC moved to or from them by contracts in block N, e.g. a stake withdrawal)
```

XDC moved by contracts is read from call traces when an RPC allows `debug_traceBlockByNumber`, which makes every figure exact. Otherwise stake withdrawals are still removed exactly, using the validator contract's `Withdraw` events, and other contract calls in that block are counted in `internal_transfers.contract_txs_unchecked`.

**Unverified figures.** A row is marked `unverified` (with a `note` saying why) when it is negative, when a contract call the owner sent or received in that block could not be traced, or when it is more than 20 % above what the owner's nodes should have earned while the block had untraced contract calls. Unverified rows are left out of totals, daily and monthly totals, yield and events; the daily files list them separately so the owner can check them.

Rewards paid at the start of epoch N are for epoch N−2.

**Several nodes, one owner.** An owner with several masternodes receives one combined amount. Each epoch also publishes the **typical node reward**: the median reward of owners running exactly one masternode, which is one node's reward exactly. Each owner row then shows `expected_xdc` (node count × typical), `vs_typical`, `shortfall_nodes` and `below_typical`.

Per-node figures are `measured` (exact) for single-node owners, and an `equal_share_estimate` otherwise, because a node's real share depends on how often it signed. When an RPC serves the protocol's own reward records, those exact figures are used instead, and `method` says `reward_files`.

## How it works

- **`worker/worker.js`** runs on Cloudflare Workers every 5 minutes. It checks the RPC endpoints, reads the network snapshot and serves both live.
- **`scripts/build_feed.py`** runs hourly in GitHub Actions. It builds the other datasets, keeps an hourly copy and history of the live feed, and commits the results. GitHub Pages serves them.
- Four times a year a second workflow squashes the git history into one commit of the current files, so the repo stays small. Data history is kept in the `history` files.
- To trace contract calls with your own archive node or a keyed provider, add its URL as the repository secret `PRIVATE_ARCHIVE_RPC`. It is used for reads only and never appears in the published files or the logs.

## Licence and disclaimer

Code: MIT. Data: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Use it freely, with credit to XDC Outpost. Data that comes from third-party services remains subject to their terms. No personal data is published.

Information only, provided as is, without warranty. Not financial, tax or investment advice. Built and run independently; not operated by the XDC Foundation or XinFin.
