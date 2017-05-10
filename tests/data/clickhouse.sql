DROP TABLE IF EXISTS csv;
CREATE TABLE csv (d Date, a String, b String) ENGINE = MergeTree(d, d, 8192);