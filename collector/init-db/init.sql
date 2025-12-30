CREATE TABLE IF NOT EXISTS currency_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_name VARCHAR(50),
    currency_name VARCHAR(100),
    details_id VARCHAR(255),
    chaos_equivalent DECIMAL(15, 2),
    pay_value DECIMAL(15, 6),
    receive_value DECIMAL(15, 6),
    trade_count INTEGER
);


CREATE TABLE IF NOT EXISTS divination_cards (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_name VARCHAR(50),
    card_name VARCHAR(255),
    stack_size INTEGER,
    chaos_value DECIMAL(15, 2),
    exalted_value DECIMAL(15, 2),
    trade_count INTEGER,
    details_id VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS unique_items (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    league_name VARCHAR(50),
    item_name VARCHAR(255),
    base_type VARCHAR(255),
    item_type VARCHAR(100), -- Ring, Amulet, Weapon, etc.
    level_required INTEGER,
    chaos_value DECIMAL(15, 2),
    links INTEGER,
    corrupted BOOLEAN,
    details_id VARCHAR(255)
);

-- 5. СОЗДАНИЕ ИНДЕКСОВ (Для ускорения будущих отчетов)

-- Индексы на время
CREATE INDEX IF NOT EXISTS idx_curr_timestamp ON currency_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_cards_timestamp ON divination_cards(timestamp);
CREATE INDEX IF NOT EXISTS idx_items_timestamp ON unique_items(timestamp);

-- Индексы на details_id
CREATE INDEX IF NOT EXISTS idx_curr_details ON currency_prices(details_id);
CREATE INDEX IF NOT EXISTS idx_cards_details ON divination_cards(details_id);
CREATE INDEX IF NOT EXISTS idx_items_details ON unique_items(details_id);

-- Составные индексы (лига + время)
CREATE INDEX IF NOT EXISTS idx_curr_league_ts ON currency_prices(league_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_cards_league_ts ON divination_cards(league_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_items_league_ts ON unique_items(league_name, timestamp);