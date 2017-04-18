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
    public $commandClass = 'bashkarev\clickhouse\command';
    /**
     * @var Schema
     */
    private $_schema;
    /**
     * @var SocketMap
     */
    private $_socketMap;

    /**
     * @inheritdoc
     */
    public function open()
    {
        $this->getSocketMap()->open();
    }

    /**
     * @inheritdoc
     */
    public function close()
    {
        if ($this->_socketMap !== null) {
            $this->_socketMap->close();
        }
    }

    /**
     * @param $sql
     * @param bool $forRead
     * @return \Generator
     */
    public function executeCommand($sql, $forRead = true)
    {
        $socket = $this->getSocketMap()->open();
        fwrite($socket, $this->createRequest($sql, $forRead));
        $this->getSocketMap()->lock($socket);
        $parser = new Parser($forRead);
        while (true) {
            $position = $parser->getPosition();
            if ($position === Parser::POS_HEADER) {
                $parser->parseHeader(fgets($socket, 1024));
            }
            if ($position === Parser::POS_LENGTH) {
                $parser->parseLength(fgets($socket, 11));
            }
            if ($position === Parser::POS_CONTENT) {
                foreach ($parser->parseContent(fread($socket, $parser->getLength())) as $value) {
                    yield $value;
                }
            }
            if ($position === Parser::POS_END) {
                fseek($socket, 2, SEEK_CUR); // \r\n end
                if (($last = $parser->getLastContent()) !== null) {
                    yield $last;
                }
                break 1;
            }
        }
        $this->getSocketMap()->unlock($socket);
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
     * @return SocketMap
     */
    public function getSocketMap()
    {
        if ($this->_socketMap === null) {
            $this->_socketMap = Yii::createObject([
                'class' => 'bashkarev\clickhouse\SocketMap',
                'db' => $this
            ]);
        }
        return $this->_socketMap;
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

    /**
     * @param string $sql
     * @param bool $forRead
     * @return string
     */
    protected function createRequest($sql, $forRead)
    {
        $data = $sql;
        if ($forRead === true) {
            $data .= ' FORMAT JSONEachRow';
        }
        $header = "POST / HTTP/1.1\r\n";
        $header .= "Content-Length: " . strlen($data) . "\r\n";
        $header .= "\r\n";
        $header .= $data;

        return $header;
    }


}