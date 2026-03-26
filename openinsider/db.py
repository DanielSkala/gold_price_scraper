import sqlite3
import threading
from openinsider.config import DATA_DIR, DB_PATH

_local = threading.local()


def get_connection() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _local.conn = sqlite3.connect(str(DB_PATH))
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
        _local.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        _local.conn.execute("PRAGMA mmap_size=268435456")  # 256MB mmap
    return _local.conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_url TEXT UNIQUE NOT NULL,
            filing_date TEXT,
            trade_date TEXT,
            ticker TEXT,
            company_name TEXT,
            insider_name TEXT,
            title TEXT,
            trade_type TEXT,
            price REAL,
            qty REAL,
            owned REAL,
            delta_own REAL,
            value REAL,
            price_change_1d REAL,
            price_change_1w REAL,
            price_change_1m REAL,
            price_change_6m REAL,
            deterministic_score REAL,
            llm_analysis TEXT,
            llm_priority TEXT,
            raw_html TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            high_52w REAL,
            low_52w REAL,
            drawdown_from_52w_high REAL,
            price_vs_200dma REAL,
            volume_ratio_20d REAL,
            market_cap REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, date)
        );

        CREATE TABLE IF NOT EXISTS insiders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company_ticker TEXT NOT NULL,
            title TEXT,
            first_seen TEXT,
            last_seen TEXT,
            total_buys INTEGER DEFAULT 0,
            total_sells INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, company_ticker)
        );

        CREATE TABLE IF NOT EXISTS clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            insider_names TEXT,
            filing_ids TEXT,
            participant_count INTEGER,
            total_value REAL,
            avg_seniority REAL,
            has_ceo INTEGER DEFAULT 0,
            has_cfo INTEGER DEFAULT 0,
            cluster_score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER,
            cluster_id INTEGER,
            channel TEXT,
            message TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            source TEXT,
            url TEXT,
            filings_found INTEGER DEFAULT 0,
            filings_new INTEGER DEFAULT 0,
            status TEXT,
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_filings_ticker ON filings(ticker);
        CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON filings(filing_date);
        CREATE INDEX IF NOT EXISTS idx_filings_trade_date ON filings(trade_date);
        CREATE INDEX IF NOT EXISTS idx_filings_insider_name ON filings(insider_name);
        CREATE INDEX IF NOT EXISTS idx_filings_trade_type ON filings(trade_type);
        CREATE INDEX IF NOT EXISTS idx_filings_deterministic_score ON filings(deterministic_score);

        -- Composite indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_filings_date_score ON filings(filing_date DESC, deterministic_score DESC);
        CREATE INDEX IF NOT EXISTS idx_filings_type_score ON filings(trade_type, deterministic_score DESC);
        CREATE INDEX IF NOT EXISTS idx_filings_type_date ON filings(trade_type, filing_date DESC);
        CREATE INDEX IF NOT EXISTS idx_filings_ticker_company ON filings(ticker, company_name);
        CREATE INDEX IF NOT EXISTS idx_clusters_date_score ON clusters(start_date, cluster_score DESC);
        CREATE INDEX IF NOT EXISTS idx_filings_date_insider ON filings(filing_date, insider_name);
    """)

    # Add is_10b5_1 column (migration for existing DBs)
    try:
        conn.execute("SELECT is_10b5_1 FROM filings LIMIT 1")
    except Exception:
        conn.execute("ALTER TABLE filings ADD COLUMN is_10b5_1 INTEGER DEFAULT NULL")
        conn.commit()

    conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_10b5_1 ON filings(is_10b5_1)")

    # FTS5 for fast search
    try:
        conn.execute("SELECT * FROM filings_fts LIMIT 1")
    except Exception:
        try:
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS filings_fts USING fts5(
                    ticker, company_name, insider_name,
                    content=filings, content_rowid=id
                )
            """)
            conn.execute("""
                INSERT INTO filings_fts(rowid, ticker, company_name, insider_name)
                SELECT id, ticker, company_name, insider_name FROM filings
            """)
            conn.commit()
        except Exception:
            pass
    conn.commit()


init_db()
