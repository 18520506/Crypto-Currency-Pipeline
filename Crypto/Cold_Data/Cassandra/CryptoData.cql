DROP KEYSPACE IF EXISTS cryptodata;
CREATE KEYSPACE cryptodata WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use cryptodata;


CREATE TABLE streaming(
    symbol text,
    time text,
    year int,
    month int,
    day int,
    hour int,
    min int,
    second int,
    currency_base text,
    price double,
    bid double,
    ask double,
    day_volume double,
    PRIMARY KEY((symbol),year,month,day,hour,min,second)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, min DESC, second DESC);

CREATE TABLE batchdata(
    symbol text,
    time text,
    year int,
    month int,
    day int,
    hour int,
    min int,
    second int,
    currency_base text,
    price double,
    bid double,
    ask double,
    day_volume double,
    PRIMARY KEY((symbol),year,month,day,hour,min,second)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, min DESC, second DESC);