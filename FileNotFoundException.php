<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\base\InvalidParamException;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class FileNotFoundException extends InvalidParamException
{
    /**
     * @inheritdoc
     */
    public function getName()
    {
        return 'File not Found';
    }
}