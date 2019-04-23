DROP TABLE IF EXISTS `csv`;
DROP TABLE IF EXISTS `customer`;
DROP TABLE IF EXISTS `types`;


CREATE TABLE csv (d Date, a String, b String) ENGINE = MergeTree(d, d, 8192);

CREATE TABLE `customer` (
  `id` UInt8,
  `email` String,
  `name` String,
  `address` String,
  `status` UInt8 DEFAULT 0,
  `profile_id` UInt8,
  `external_id` UInt64 DEFAULT 0
) ENGINE=Memory;

INSERT INTO `customer` (id, email, name, address, status, profile_id) VALUES (1,'user1@example.com', 'user1', 'address1', 1, 1);
INSERT INTO `customer` (id, email, name, address, status) VALUES (2, 'user2@example.com', 'user2', 'address2', 1);
INSERT INTO `customer` (id, email, name, address, status, profile_id) VALUES (3, 'user3@example.com', 'user3', 'address3', 2, 2);

CREATE TABLE `types` (
 `UInt8` UInt8,
 `UInt16` UInt16,
 `UInt32` UInt32,
 `UInt64` UInt64,
 `Int8` Int8,
 `Int16` Int16,
 `Int32` Int32,
 `Int64` Int64,
 `Float32` Float32,
 `Float64` Float64,
 `String` String,
 `FixedString` FixedString(20),
 `DateTime` DateTime,
 `Date` Date,
 `Enum8` Enum8('hello' = 1, 'world' = 2),
 `Enum16` Enum8('hello' = 1, 'world' = 2),
 `Decimal9_2` Decimal(9, 2),
 `Decimal18_4` Decimal(18, 4),
 `Decimal38_10` Decimal(38, 10)
) ENGINE=Memory;