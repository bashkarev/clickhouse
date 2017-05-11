DROP TABLE IF EXISTS `csv`;
DROP TABLE IF EXISTS `customer`;


CREATE TABLE csv (d Date, a String, b String) ENGINE = MergeTree(d, d, 8192);

CREATE TABLE `customer` (
  `id` UInt8,
  `email` String,
  `name` String,
  `address` String,
  `status` UInt8 DEFAULT 0,
  `profile_id` UInt8
) ENGINE=Memory;

INSERT INTO `customer` (id, email, name, address, status, profile_id) VALUES (1,'user1@example.com', 'user1', 'address1', 1, 1);
INSERT INTO `customer` (id, email, name, address, status) VALUES (2, 'user2@example.com', 'user2', 'address2', 1);
INSERT INTO `customer` (id, email, name, address, status, profile_id) VALUES (3, 'user3@example.com', 'user3', 'address3', 2, 2);