CREATE TABLE IF NOT EXISTS events (
    content_hash TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    title        TEXT NOT NULL,
    body         TEXT NOT NULL,
    url          TEXT NOT NULL,
    published_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker);
CREATE INDEX IF NOT EXISTS idx_events_published ON events(published_at);

CREATE TABLE IF NOT EXISTS price_labels (
    content_hash TEXT NOT NULL,
    horizon      TEXT NOT NULL,
    base_price   REAL NOT NULL,
    future_price REAL NOT NULL,
    return_pct   REAL NOT NULL,
    PRIMARY KEY (content_hash, horizon),
    FOREIGN KEY (content_hash) REFERENCES events(content_hash)
);
