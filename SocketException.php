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
class SocketException extends Exception
{

    /**
     * @inheritdoc
     */
    public function __construct($message, $errorInfo = [], $code = 0, \Exception $previous = null)
    {
        if ($code === 0) {
            $code = $this->parseCode($message);
        }
        parent::__construct($message, $errorInfo, $code, $previous);
    }

    /**
     * @param $message
     * @return int
     */
    protected function parseCode($message)
    {
        if (preg_match('/errno=(\d+)/', $message, $out)) {
            return (int)$out[1];
        }
        return 0;
    }

}