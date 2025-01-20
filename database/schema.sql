CREATE DATABASE `stock-wizard`;

CREATE TABLE `tickers` (
    `symbol` VARCHAR(20) PRIMARY KEY,  -- 股票代码，设为主键
    `name` VARCHAR(255) NOT NULL,      -- 股票名称
    `region` VARCHAR(50),              -- 地区（如 US、HK、CN）
    `exchange` VARCHAR(50),            -- 市场（如 NASDAQ、NYSE、上交所A股）
    `ipo_date` DATE,                   -- IPO Date
    `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- 更新时间，自动更新
);

CREATE TABLE daily_stock_prices
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