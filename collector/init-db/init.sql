
-- MASTER TABLE: Leagues

CREATE TABLE IF NOT EXISTS leagues (
    id SERIAL PRIMARY KEY,
    league_name VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'Active' CHECK (status IN ('Active', 'Expired')),
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast league lookups by name
CREATE INDEX IF NOT EXISTS idx_leagues_name ON leagues(league_name);
-- Index for filtering by status
CREATE INDEX IF NOT EXISTS idx_leagues_status ON leagues(status);

-- DATA TABLES (Linked to Leagues)

-- Table A: Currency Prices
CREATE TABLE IF NOT EXISTS currency_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_id INTEGER NOT NULL REFERENCES leagues(id) ON DELETE RESTRICT,
    currency_name VARCHAR(100) NOT NULL,
    details_id VARCHAR(255),
    chaos_equivalent DECIMAL(15, 2),
    pay_value DECIMAL(15, 6),
    receive_value DECIMAL(15, 6),
    trade_count INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table B: Divination Cards
CREATE TABLE IF NOT EXISTS divination_cards (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_id INTEGER NOT NULL REFERENCES leagues(id) ON DELETE RESTRICT,
    card_name VARCHAR(255) NOT NULL,
    stack_size INTEGER,
    chaos_value DECIMAL(15, 2),
    trade_count INTEGER,
    details_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table C: Unique Items
CREATE TABLE IF NOT EXISTS unique_items (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_id INTEGER NOT NULL REFERENCES leagues(id) ON DELETE RESTRICT,
    item_name VARCHAR(255) NOT NULL,
    base_type VARCHAR(255),
    item_type VARCHAR(100), -- Ring, Amulet, Weapon, etc.
    level_required INTEGER,
    chaos_value DECIMAL(15, 2),
    links INTEGER,
    details_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

-- PERFORMANCE INDEXES

-- Single-column indexes on timestamp (for time-series queries)
CREATE INDEX IF NOT EXISTS idx_curr_timestamp ON currency_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_cards_timestamp ON divination_cards(timestamp);
CREATE INDEX IF NOT EXISTS idx_items_timestamp ON unique_items(timestamp);

-- Indexes on details_id (for API correlation)
CREATE INDEX IF NOT EXISTS idx_curr_details ON currency_prices(details_id);
CREATE INDEX IF NOT EXISTS idx_cards_details ON divination_cards(details_id);
CREATE INDEX IF NOT EXISTS idx_items_details ON unique_items(details_id);

-- Composite indexes on league_id + timestamp (optimized for league specific time-series)
CREATE INDEX IF NOT EXISTS idx_curr_league_ts ON currency_prices(league_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_cards_league_ts ON divination_cards(league_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_items_league_ts ON unique_items(league_id, timestamp);

-- Additional indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_curr_league_name ON currency_prices(league_id, currency_name);
CREATE INDEX IF NOT EXISTS idx_cards_league_name ON divination_cards(league_id, card_name);
CREATE INDEX IF NOT EXISTS idx_items_league_name ON unique_items(league_id, item_name);

-- TRIGGER FOR AUTOMATIC UPDATED_AT ON LEAGUES
CREATE OR REPLACE FUNCTION update_leagues_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_leagues_updated_at
    BEFORE UPDATE ON leagues
    FOR EACH ROW
    EXECUTE FUNCTION update_leagues_updated_at();

-- BACKWARD COMPATIBILITY

-- These views provide league_name directly in data tables if needed
CREATE OR REPLACE VIEW currency_prices_with_league AS
SELECT 
    cp.*,
    l.league_name,
    l.status AS league_status
FROM currency_prices cp
JOIN leagues l ON cp.league_id = l.id;

CREATE OR REPLACE VIEW divination_cards_with_league AS
SELECT 
    dc.*,
    l.league_name,
    l.status AS league_status
FROM divination_cards dc
JOIN leagues l ON dc.league_id = l.id;

CREATE OR REPLACE VIEW unique_items_with_league AS
SELECT 
    ui.*,
    l.league_name,
    l.status AS league_status
FROM unique_items ui
JOIN leagues l ON ui.league_id = l.id;