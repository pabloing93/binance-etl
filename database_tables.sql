DROP TABLE IF EXISTS pabloing1993_coderhouse.bitcoin_candles;
create table pabloing1993_coderhouse.bitcoin_candles(
	load_time DATETIME,
	open_time DATETIME,
	open_price FLOAT,
	high_price FLOAT,
	low_price FLOAT,
	close_price FLOAT,
	volume FLOAT,
	close_time DATETIME,
	trades INT,
	trend VARCHAR
);