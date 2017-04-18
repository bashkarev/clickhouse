<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\base\Object;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class SocketMap extends Object
{
    /**
     * @var Connection
     */
    public $db;
    /**
     * @var array
     */
    private $_sockets = [];
    /**
     * @var array
     */
    private $_lock;

    /**
     * @return resource
     * @throws Exception
     */
    public function open()
    {
        foreach ($this->_sockets as $socket) {
            $id = (int)$socket;
            if (!isset($this->_lock[$id])) {
                return $socket;
            }
        }
        return $this->create();
    }

    /**
     * Close all socket connection
     */
    public function close()
    {
        foreach ($this->_sockets as $socket) {
            \Yii::trace("Closing clickhouse DB connection: {$this->db->dsn} ($socket)", __METHOD__);
            @fwrite($socket, "Connection: Close \r\n");
            stream_socket_shutdown($socket, STREAM_SHUT_RDWR);
        }
        $this->_sockets = [];
    }

    /**
     * Lock resource
     * @param resource $socket
     */
    public function lock($socket)
    {
        $this->_lock[(int)$socket] = true;
    }

    /**
     * Unlock resource
     * @param resource $socket
     */
    public function unlock($socket)
    {
        unset($this->_lock[(int)$socket]);
    }

    /**
     * @return resource
     * @throws Exception
     */
    protected function create()
    {
        $socket = @stream_socket_client($this->db->dsn, $code, $message);
        if ($socket === false) {
            throw new Exception($code, $message);
        }
        \Yii::trace("Opening clickhouse DB connection: {$this->db->dsn} ($socket)", __METHOD__);
        $this->_sockets[] = $socket;
        return $socket;
    }

}