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
class Parser
{
    const POS_HEADER = 0x01;
    const POS_LENGTH = 0x02;
    const POS_CONTENT = 0x03;
    const POS_END = 0x04;

    const CRLF = "\r\n";

    /**
     * @var int
     */
    protected $position = 0x01;
    /**
     * @var bool
     */
    protected $forRead;
    /**
     * @var int
     */
    protected $httpCode;
    /**
     * @var integer
     */
    protected $length;
    /**
     * @var string
     */
    protected $last;

    /**
     * @param bool $forRead
     */
    public function __construct($forRead)
    {
        $this->forRead = $forRead;
    }

    /**
     * @param resource $socket
     * @return \Generator
     */
    public function run($socket)
    {
        for (; ;) {
            if ($this->position === self::POS_HEADER) {
                $line = fgets($socket, 1024);
                if ($line === false) {
                    continue;
                }
                $this->parseHeader($line);
            }
            if ($this->position === self::POS_LENGTH) {
                $line = fgets($socket, 11);
                if ($line === false || $line === self::CRLF) {
                    continue;
                }
                $this->parseLength($line);
            }
            if ($this->position === self::POS_CONTENT) {
                foreach ($this->parseContent(fread($socket, $this->length)) as $value) {
                    yield $value;
                }
            }
            if ($this->position === self::POS_END) {
                fseek($socket, 2, SEEK_CUR); // \r\n end
                if (($last = $this->getLastContent()) !== null) {
                    yield $last;
                }
                break 1;
            }
        }
    }

    /**
     * @param $buffer
     * @return \Generator
     * @throws Exception
     */
    protected function parseContent($buffer)
    {
        if ($this->httpCode !== 200) {
            throw new Exception($buffer);
        }
        $lines = explode("\n", $buffer);
        $count = count($lines) - 1;
        for ($i = 0; ; $i++) {
            if ($i === $count) {
                $this->last = $lines[$i];
                break 1;
            }

            $line = $lines[$i];
            if ($i === 0 && $this->last !== null) {
                $line = $this->last . $line;
                $this->last = null;
            }
            $value = $this->parseContentLine($line);
            if ($value !== null) {
                yield $value;
            }
        }
        $this->position = self::POS_LENGTH;
    }

    /**
     * @param $value
     * @return array|null
     */
    protected function parseContentLine($value)
    {
        if ($this->forRead === false || $value === '') {
            return null;
        }

        return json_decode($value, true);
    }

    /**
     * @return mixed
     */
    protected function getLastContent()
    {
        if ($this->last === null) {
            return null;
        }
        return $this->parseContentLine($this->last);
    }

    /**
     * @param $line
     */
    protected function parseHeader($line)
    {
        if ($this->httpCode === null) {
            $this->parseCode($line);
        }

        if ($line === self::CRLF) {
            $this->position = self::POS_LENGTH;
        }
    }

    /**
     * @param $line
     */
    protected function parseLength($line)
    {
        $this->length = hexdec($line);
        $this->position = ($this->length === 0) ? self::POS_END : self::POS_CONTENT;
    }

    /**
     * @param string $line
     */
    protected function parseCode($line)
    {
        $this->httpCode = (int)substr($line, 9, 3);
    }

}
