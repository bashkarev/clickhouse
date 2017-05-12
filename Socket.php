<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use Yii;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Socket
{
    /**
     * @var resource
     */
    protected $socket;
    /**
     * @var Configuration
     */
    protected $config;
    /**
     * @var bool
     */
    private $_lock = false;

    public function __construct(Configuration $config)
    {
        $this->config = $config;
        $this->open();
    }

    public function __wakeup()
    {
        $this->open();
    }

    public function open()
    {
        $this->socket = @stream_socket_client($this->config->getAddress(), $code, $message);
        if ($this->socket === false) {
            throw new Exception($message, [], $code);
        }
        if (stream_set_blocking($this->socket, false) === false) {
            throw new Exception('Failed set non blocking socket');
        }
        if (YII_DEBUG) {
            Yii::trace("Opening clickhouse DB connection: " . $this->config->getAddress() . "($this->socket)", __METHOD__);
        }
    }

    public function close()
    {
        if (YII_DEBUG) {
            Yii::trace("Closing clickhouse DB connection: " . $this->config->getAddress() . "($this->socket)", __METHOD__);
        }
        @fwrite($this->socket, "Connection: Close \r\n");
        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);
    }

    /**
     * Lock
     */
    public function lock()
    {
        $this->_lock = true;
    }

    /**
     * Unlock
     */
    public function unlock()
    {
        $this->_lock = false;
    }

    /**
     * @return bool
     */
    public function isLocked()
    {
        return $this->_lock;
    }

    /**
     * @return resource
     */
    public function getNative()
    {
        return $this->socket;
    }
}