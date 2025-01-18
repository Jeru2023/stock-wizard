CREATE DATABASE `stock-wizard`;
CREATE TABLE `tickers` (
    `symbol` VARCHAR(20) PRIMARY KEY,  -- 股票代码，设为主键
    `name` VARCHAR(255) NOT NULL,      -- 股票名称
    `region` VARCHAR(50),              -- 地区（如 US、HK、CN）
    `exchange` VARCHAR(50),            -- 市场（如 NASDAQ、NYSE、上交所A股）
    `ipo_date` DATE,                   -- IPO Date
    `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- 更新时间，自动更新
);