<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\helpers;

use yii\helpers\StringHelper;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Csv
{

    const EOL = "\n";

    /**
     * @param mixed $value
     * @return string
     */
    public static function toString($value)
    {
        $str = null;
        foreach ($value as $item) {
            if ($str !== null) {
                $str .= ',';
            }
            $type = 'set' . gettype($item);
            $str .= static::$type($item);
        }
        return $str;
    }

    /**
     * @param resource $handle
     * @param array $fields
     * @return int|bool
     */
    public static function write($handle, $fields)
    {
        $line = static::toString($fields);
        if ($line === '') {
            return 0;
        }
        return fwrite($handle, $line . self::EOL);
    }

    /**
     * @param bool $value
     * @return int
     */
    protected static function setBoolean($value)
    {
        return (int)$value;
    }

    /**
     * @param null $value
     * @return string
     */
    protected static function setNull($value)
    {
        return '';
    }

    /**
     * @param int $value
     * @return int
     */
    protected static function setInteger($value)
    {
        return $value;
    }

    /**
     * @param float $value
     * @return string
     */
    protected static function setDouble($value)
    {
        return StringHelper::normalizeNumber($value);
    }

    /**
     * @param string $value
     * @return string
     */
    protected static function setString($value)
    {
        if (
            strpos($value, ',') !== false
            || strpos($value, '"') !== false
        ) {
            $value = '"' . str_replace('"', '""', $value) . '"';
        }
        return addcslashes($value, "\r\n");
    }

    /**
     * toDo
     * @param object|array $value
     */
    protected static function setArray($value)
    {
        throw new \RuntimeException('Type `array` is not supported');
    }

    /**
     * @param object $value
     */
    protected static function setObject($value)
    {
        return static::setArray($value);
    }

    /**
     * @throws \RuntimeException
     */
    protected static function setResource()
    {
        throw new \RuntimeException('Type `resource` is not supported');
    }

}