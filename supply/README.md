# Supply queue trigger

The `supply` queue loads CAISO daily supply data into `ciso.supply`.

```json
{"mode":"forward","date":"2026-11-21","until":"2040-01-01"}
```

Forward mode starts at the given Pacific date, catches up in batches of at
most 31 days, then refreshes yesterday and today every five minutes. The
optional `until` is an inclusive Pacific date: polling continues throughout
that date and stops after its final refresh on the following day. Omit it for
unbounded polling. Dates cover complete daily source files, so restarting an
incomplete day deliberately reloads its earlier intervals.

Backfills use `{"mode":"backfill","date":"2026-11-18","until":"2021-01-01"}`;
both dates are inclusive. Failures use the
[shared retry/poison policy](../rtprices/README.md#retry-policy-all-queue-triggers).
