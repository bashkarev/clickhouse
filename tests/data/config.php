<?php

$config = [
    'database' => [
        'class' => 'bashkarev\clickhouse\Connection',
        'dsn' => 'host=127.0.0.1;database=default;port=8123;',
        'username' => 'default',
        'password' => '',
        'fixture' => __DIR__ . '/clickhouse.sql',
    ]
];

if (is_file(__DIR__ . '/config.local.php')) {
    include(__DIR__ . '/config.local.php');
}

return $config;