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
     * @return int
     */
    public function getPosition()
    {
        return $this->position;
    }

    /**
     * @return int
     */
    public function getLength()
    {
        return $this->length;
    }

    /**
     * @param $buffer
     * @return \Generator
     * @throws Exception
     */
    public function parseContent($buffer)
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
    public function getLastContent()
    {
        if ($this->last) {
            return null;
        }
        return $this->parseContentLine($this->last);
    }

    /**
     * @return bool
     */
    public function isEnd()
    {
        return $this->position === self::POS_END;
    }

    /**
     * @param $line
     */
    public function parseHeader($line)
    {
        if($line === false){
            return;
        }
        $line = rtrim($line, " \n\r");
        if ($this->httpCode === null) {
            $this->parseCode($line);
        }

        if ($line === '') {
            $this->position = self::POS_LENGTH;
        }
    }

    /**
     * @param $line
     */
    public function parseLength($line)
    {
        $line = rtrim($line, " \n\r");
        if ($line === '') {
            return;
        }

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
