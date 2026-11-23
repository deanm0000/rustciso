CREATE TABLE ciso.load_fcst (
    utcbegin TIMESTAMPTZ,
    market VARCHAR,
    exec_type VARCHAR,
    tac_area VARCHAR,
    mw DOUBLE PRECISION,
    downloaded_at TIMESTAMPTZ
) USING ducklake;

CALL ducklake.set_partition('ciso.load_fcst', 'market');

CREATE TABLE ciso.load_fcst_rtpd (
    utcbegin TIMESTAMPTZ,
    tac_area VARCHAR,
    mw DOUBLE PRECISION,
    downloaded_at TIMESTAMPTZ
) USING ducklake;

CREATE TABLE ciso.load_fcst_rtm (
    utcbegin TIMESTAMPTZ,
    tac_area VARCHAR,
    mw DOUBLE PRECISION,
    downloaded_at TIMESTAMPTZ
) USING ducklake;
