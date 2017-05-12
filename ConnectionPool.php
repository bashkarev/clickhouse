<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class ConnectionPool
{
    /**
     * @var Connection
     */
    public $db;
    /**
     * @var Socket[]
     */
    private $_sockets = [];
    /**
     * @var Socket
     */
    private $_main;

    public function __construct(Connection $db)
    {
        $this->db = $db;
    }

    /**
     * @return Socket
     * @throws Exception
     */
    public function open()
    {
        $main = $this->getMain();
        if ($main->isLocked() === false) {
            return $main;
        }
        foreach ($this->_sockets as $socket) {
            if ($socket->isLocked() === false) {
                return $socket;
            }
        }
        $socket = new Socket($this->db->getConfiguration());
        $this->_sockets[] = $socket;
        return $socket;
    }

    /**
     * @return Socket
     */
    protected function getMain()
    {
        if ($this->_main === null) {
            $this->_main = new Socket($this->db->getConfiguration());
        }
        return $this->_main;
    }

    /**
     * Close all socket connection
     */
    public function close()
    {
        if ($this->_main === null) {
            return;
        }
        $this->_main->close();
        foreach ($this->_sockets as $socket) {
            $socket->close();
        }
        $this->_main = null;
        $this->_sockets = [];
    }

    /**
     * @return int total open socket connections
     */
    public function total()
    {
        if ($this->_main === null) {
            return 0;
        }
        return count($this->_sockets) + 1;
    }

}