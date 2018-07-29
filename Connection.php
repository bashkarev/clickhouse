<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use Yii;
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
    public $schemaMap;
    /**
     * @inheritdoc
     */
    public $commandClass = 'bashkarev\clickhouse\Command';
    /**
     * @var Schema
     */
    private $_schema;
    /**
     * @var ConnectionPool
     */
    private $_pool;
    /**
     * @var Configuration
     */
    private $_configuration;

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
     */
    public function open()
    {
        $this->getPool()->open();
    }

    /**
     * @inheritdoc
     */
    public function close()
    {
        if ($this->_pool !== null) {
            $this->_pool->close();
        }
    }

    /**
     * @return \Generator
     */
    public function execute()
    {
        $socket = $this->getPool()->open();
        $socket->lock();
        while (true) {
            $data = yield;
            if ($data === false) {
                break 1;
            }
            $socket->write($data);
        }
        yield from (new Parser())->run($socket->getNative());
        $socket->unlock();
    }

    /**
     * @return Schema
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
     * @return ConnectionPool
     */
    public function getPool()
    {
        if ($this->_pool === null) {
            $this->_pool = new ConnectionPool($this);
        }
        return $this->_pool;
    }

    /**
     * @return Configuration
     */
    public function getConfiguration()
    {
        if ($this->_configuration === null) {
            $this->_configuration = new Configuration($this->dsn, $this->username, $this->password);
        }
        return $this->_configuration;
    }

    /**
     * @inheritdoc
     */
    public function getIsActive()
    {
        return ($this->_pool !== null && $this->_pool->total() !== 0);
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
