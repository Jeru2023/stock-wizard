CREATE DATABASE `stock-wizard`;

CREATE DATABASE `stock-wizard`;

CREATE TABLE `tickers` (
    `symbol` VARCHAR(20) PRIMARY KEY,  -- Stock Symbol，Primary Key
    `name` VARCHAR(255) NOT NULL,      -- Stock Name
    `region` VARCHAR(50),              -- 地区（如 US、HK、CN）
    `exchange` VARCHAR(50),            -- 市场（如 NASDAQ、NYSE、A股）
    `ipo_date` DATE,                   -- IPO Date
    `status` VARCHAR(255),             -- Status
    `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- 更新时间，自动更新
);
CREATE INDEX idx_status ON tickers (status);

CREATE TABLE daily_stock_prices_realtime
(
    date      date           not null,
    symbol    varchar(10)    not null,
    adj_close decimal(15, 4) null,
    close     decimal(15, 4) null,
    high      decimal(15, 4) null,
    low       decimal(15, 4) null,
    open      decimal(15, 4) null,
    volume    bigint         null,
    primary key (date, symbol)
);
CREATE INDEX idx_symbol_date ON daily_stock_prices_realtime (symbol, date);

CREATE TABLE daily_stock_prices_history
(
    date      date           not null,
    symbol    varchar(10)    not null,
    adj_close decimal(15, 4) null,
    close     decimal(15, 4) null,
    high      decimal(15, 4) null,
    low       decimal(15, 4) null,
    open      decimal(15, 4) null,
    volume    bigint         null,
    primary key (date, symbol)
);
CREATE INDEX idx_symbol_date ON daily_stock_prices_history (symbol, date);

CREATE TABLE daily_stock_moving_averages (
    date           date           NOT NULL,
    symbol         varchar(10)    NOT NULL,
    current_price  decimal(15, 4) NULL,  -- 当前股价（adj_close）
    ma_50          decimal(15, 4) NULL,  -- 50 日均线
    ma_150         decimal(15, 4) NULL,  -- 150 日均线
    ma_200         decimal(15, 4) NULL,  -- 200 日均线
    high_of_52weeks decimal(15, 4) NULL,  -- 52周最高
    low_of_52weeks decimal(15, 4) NULL,  -- 52周最低
    PRIMARY KEY (date, symbol)
);