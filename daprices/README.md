# Daily DAM prices queue trigger

Loads OASIS `PRC_LMP` (version 12, `market_run_id=DAM`,
`grp_type=ALL_APNODES`) into `ciso.daprices`, pivoting LMP types into
`dalmp`, `damcc`, `damce`, `damcl` and `damghg`.

Enqueue messages on the `daprices` queue using the same format as
[rtprices](../rtprices/README.md), with these differences:

- Each job loads one whole `America/Los_Angeles` operating day (23-25 hours
  on daylight-saving days). `startdatetime` must be local midnight, e.g.
  `20261016T07:00-0000` (PDT) or `20261216T08:00-0000` (PST); other values
  are rejected.
- `get_next_until` / `get_prev_until` step one local day at a time.
- Continuations become visible at 14:00 Pacific the day before the
  operating day (DAM results normally post around 13:00).
- Not-ready responses retry every 10 minutes.
- Every day is both the first and last window of its date, so either repack
  flag queues a repack of that day with `table: "ciso.daprices"`.
