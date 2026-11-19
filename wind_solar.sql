CREATE TABLE ciso.wind_solar (
    utcbegin TIMESTAMPTZ,
    market_type VARCHAR,
    trading_hub VARCHAR,
    renewable_type VARCHAR,
    mw DOUBLE PRECISION,
    downloaded_at TIMESTAMPTZ -- Generated at ingestion; no OASIS source column.
) USING ducklake;

CALL ducklake.set_partition('ciso.wind_solar', 'market_type');
