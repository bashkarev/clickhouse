Extension ClickHouse for Yii 2
==============================

This extension provides the [ClickHouse](https://clickhouse.yandex/) integration for the [Yii framework 2.0](http://www.yiiframework.com).

[![Build Status](https://travis-ci.org/bashkarev/clickhouse.svg?branch=master)](https://travis-ci.org/bashkarev/clickhouse)

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
        'dsn' => 'host=localhost;port=8823;database=default;connect_timeout_with_failover_ms=10',
        'username' => 'default',
        'password' => 'default'
    ]
];
```

[All settings](https://clickhouse.yandex/reference_en.html#Settings)


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

Insert csv files
----------------

> Files are uploaded in parallel.

```php 
/**
 * @var \bashkarev\clickhouse\InsertFiles $insert
 */
$insert = Yii::$app->clickhouse->createCommand()->batchInsertFiles('csv',[
    '@vendor/bashkarev/clickhouse/tests/data/csv/e1e747f9901e67ca121768b36921fbae.csv',
    '@vendor/bashkarev/clickhouse/tests/data/csv/ebe191dfc36d73aece91e92007d24e3e.csv',
]);
$insert
    ->setFiles(fopen('/csv/ebe191dfc36d73aece91e92007d24e3e.csv','rb'))
    ->setChunkSize(8192) // default 4096 bytes
    ->execute();
```

