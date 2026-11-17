# Intertie shadow-price queue trigger

The `intertie_shadows` function consumes the `intertie-shadows` Azure Queue and
writes `ciso.intertie_shadows`. Job format, Pacific-month defaults, continuation
rules, retries, and market-scoped storage behavior are shared with
[nomogram_shadows](../nomogram_shadows/README.md).

| `market_run_id` / stored `market_type` | OASIS query | Version | Upstream market | Interval |
|---|---|---|---|---|
| `dam` | `PRC_CNSTR` | 13 | DAM | 60 minutes |
| `hasp` | `PRC_CNSTR` | 13 | HASP | 60 minutes |
| `rtm` | `PRC_CNSTR` | 13 | RTM | 15 minutes |
| `ruc` | `PRC_CNSTR` | 13 | RUC | 60 minutes |
| `interval` | `PRC_RTM_FLOWGATE` | 12 | RTM | 5 minutes |

All requests use `SingleZip`, `resultformat=6`, `ti_id=ALL`, and
`ti_direction=ALL`. `LIMIT_TYPE` and `PRODUCT` remain nullable columns;
direction and constraint cause are preserved, not aggregated away.

Use the intertie table grain from [shadow_prices.sql](../shadow_prices.sql)
when configuring deduplication. Use `intertie` rather than `nomogram` as the
row-group column in its dkplus table tags.
