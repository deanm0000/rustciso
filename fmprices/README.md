# Hourly FMM (RTPD) prices queue trigger

Loads OASIS `PRC_RTPD_LMP` (version 3, `market_run_id=RTPD`,
`grp_type=ALL_APNODES`) into `ciso.fmprices`, pivoting LMP types into
`fmlmp`, `fmmcc`, `fmmce`, `fmmcl` and `fmmghg`.

Enqueue messages on the `fmprices` queue. The message format, continuations,
readiness timing, retries and repack behavior are identical to
[rtprices](../rtprices/README.md), except repack messages use
`table: "ciso.fmprices"`.
