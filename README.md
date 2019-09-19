Extension ClickHouse for Yii 2
==============================

This extension provides the [ClickHouse](https://clickhouse.yandex/) integration for the [Yii framework 2.0](http://www.yiiframework.com).
Main features:
- SQL commands
- Query builder
- Schema builder
- Migrations
- Batch Insert
- Parallel insert from large CSV files
- Valid handling of UInt64 type in PHP
- Supports Decimals and Nullable fields

[![Build Status](https://travis-ci.org/olexainf/clickhouse.svg?branch=master)](https://travis-ci.org/olexainf/clickhouse)

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
composer require bashkarev/clickhouse
```


Configuration
-------------

To use this extension, simply add the following code in your application configuration:

```php
return [
    //....
    'clickhouse' => [
        'class' => 'bashkarev\clickhouse\Connection',
        'dsn' => 'host=localhost;port=8123;database=default;connect_timeout_with_failover_ms=10',
        'username' => 'default',
        'password' => '',
    ],
];
```

[All settings](https://clickhouse.yandex/docs/en/operations/settings/index.html)


Using DebugPanel
----------------

Add the following to you application config to enable it (if you already have the debug module
enabled, it is sufficient to just add the panels configuration):

```php
    // ...
    'bootstrap' => ['debug'],
    'modules' => [
        'debug' => [
            'class' => 'yii\\debug\\Module',
            'panels' => [
                'clickhouse' => [
                    'class' => 'bashkarev\clickhouse\debug\Panel',
                    // 'db' => 'clickhouse', // ClickHouse component ID, defaults to `db`. Uncomment and change this line, if you registered component with a different ID.
                ],
            ],
        ],
    ],
    // ...
```

Using Migrations
----------------

In order to enable this command you should adjust the configuration of your console application:

```php
return [
    // ...
    'controllerMap' => [
        'clickhouse-migrate' => 'bashkarev\clickhouse\console\controllers\MigrateController'
    ],
];
```

```bash
# creates a new migration named 'create_target'
yii clickhouse-migrate/create create_target

# applies ALL new migrations
yii clickhouse-migrate

# reverts the last applied migration
yii clickhouse-migrate/down
```
Access to native SMI2 ClickHouse client
---------------------------------------
```php
$client = \Yii::$app->clickhouse->getClient();
```

Insert csv files
----------------

> Files are uploaded in parallel.

```php
$db = \Yii::$app->clickhouse;
$client = $db->getClient();

$results = $client->insertBatchFiles('table_name', ['file_with_data.csv']);

$state = $results['file_with_data.csv'];
$isSuccess = !$state->isError();
$uploadInfo = $state->responseInfo();

print_r($uploadInfo);
```

