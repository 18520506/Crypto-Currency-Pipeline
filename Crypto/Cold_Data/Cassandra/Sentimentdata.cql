DROP KEYSPACE IF EXISTS sentiment;
CREATE KEYSPACE sentiment WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use sentiment;


CREATE TABLE data(
    symbol text,
    dtime text,
    bearish_count int,
    bullish_count int,
    PRIMARY KEY((symbol))
);