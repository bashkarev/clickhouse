<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use Yii;

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

    /**
     * Open socket connection
     * @throws SocketException
     */
    public function open()
    {
        $this->socket = @stream_socket_client($this->config->getAddress(), $code, $message);
        if ($this->socket === false) {
            throw new SocketException($message, [], $code);
        }
        if (stream_set_blocking($this->socket, false) === false) {
            throw new SocketException('Failed set non blocking socket');
        }
        if (YII_DEBUG) {
            Yii::trace("Opening clickhouse DB connection: " . $this->config->getAddress() . " ($this->socket)", __METHOD__);
        }
    }

    /**
     * Close socket connection
     */
    public function close()
    {
        if (YII_DEBUG) {
            Yii::trace("Closing clickhouse DB connection: " . $this->config->getAddress() . " ($this->socket)", __METHOD__);
        }
        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);
    }

    /**
     * @param string $string
     * @param null|int $length
     * @return int
     * @throws SocketException
     */
    public function write($string, $length = null)
    {
        if ($length === null) {
            $length = strlen($string);
        }

        $bytes = @fwrite($this->socket, $string);
        if ($bytes === false) {
            $message = "Failed to write to socket";
            if ($error = error_get_last()) {
                $message .= sprintf(" Errno: %d; %s", $error["type"], $error["message"]);
            }
            throw new SocketException($message);
        }
        if ($bytes !== $length) {
            $this->write(substr($string, $bytes));
        }
        return $bytes;
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