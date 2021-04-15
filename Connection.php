<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use ClickHouseDB\Client;
use Yii;
use yii\base\InvalidConfigException;
use yii\base\NotSupportedException;

/**
 * @method Command createCommand($sql = null, $params = [])
 *
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Connection extends \yii\db\Connection
{
    /**
     * @inheritdoc
     */
    public $commandClass = 'bashkarev\clickhouse\Command';

    /**
     * Additional options you can pass into clickhouse client constructor
     *
     * @var array
     */
    public $clientOptions = [];

    /**
     * @var Client
     */
    private $_client;

    /**
     * @var Schema
     */
    private $_schema;

    /**
     * @inheritdoc
     */
    public function __construct($config = [])
    {
        if (!isset($config['driverName'])) {
            $config['driverName'] = 'clickhouse';
        }

        parent::__construct($config);
    }
    
    /**
     * @inheritdoc
     * @throws InvalidConfigException
     */
    public function open()
    {
        if ($this->_client === null) {
            $config = $this->parseDsn();

            $this->_client = new Client(array_merge([
                'host' => $config['host'] ?? '127.0.0.1',
                'port' => $config['port'] ?? 8123,
                'username' => $this->username,
                'password' => $this->password,
            ], $this->clientOptions),
                array_merge([
                    'database' => $config['database'] ?? 'default',
                ], $this->attributes ?? [])
            );
        }
    }

    /**
     * @throws InvalidConfigException
     */
    private function parseDsn(): array
    {
        $parts = explode(';', $this->dsn);
        $config = [];
        foreach ($parts as $part) {
            $paramValue = explode('=', $part);

            if (empty($paramValue[0])) {
                throw new InvalidConfigException("Invalid (empty) param name in dsn");
            }

            if (empty($paramValue[1])) {
                throw new InvalidConfigException("Invalid (empty) param '{$paramValue[0]}' value in dsn");
            }

            $config[$paramValue[0]] = $paramValue[1];
        }

        return $config;
    }

    /**
     * @inheritdoc
     */
    public function close()
    {
        if ($this->_client !== null) {
            $this->_client = null;
        }
    }

    /**
     * @param string $sql
     * @return \ClickHouseDB\Statement
     */
    public function execute(string $sql)
    {
        $this->open();

        return $this->_client->write($sql);
    }

    /**
     * @param string $sql
     * @return \ClickHouseDB\Statement
     * @throws \Exception
     */
    public function executeSelect(string $sql)
    {
        $this->open();

        return $this->_client->select($sql);
    }

    /**
     * @return Schema
     * @throws InvalidConfigException
     */
    public function getSchema()
    {
        if ($this->_schema === null) {
            $this->_schema = Yii::createObject([
                'class' => 'bashkarev\clickhouse\Schema',
                'db' => $this
            ]);
        }
        return $this->_schema;
    }

    /**
     * @inheritdoc
     */
    public function getIsActive()
    {
        return ($this->_client !== null);
    }

    public function getClient(): Client
    {
        return $this->_client;
    }

    /**
     * @inheritdoc
     * @throws NotSupportedException
     */
    public function beginTransaction($isolationLevel = null)
    {
        throw new NotSupportedException('In the clickhouse database, transactions are not supported');
    }

    /**
     * @inheritdoc
     * @throws NotSupportedException
     */
    public function getTransaction($isolationLevel = null)
    {
        throw new NotSupportedException('In the clickhouse database, transactions are not supported');
    }

    /**
     * @inheritdoc
     * @throws NotSupportedException
     */
    public function transaction(callable $callback, $isolationLevel = null)
    {
        throw new NotSupportedException('In the clickhouse database, transactions are not supported');
    }

}
