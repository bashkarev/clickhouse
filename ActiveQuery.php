<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class ActiveQuery extends \yii\db\ActiveQuery
{
    /**
     * @inheritdoc
     */
    public function each($batchSize = 100, $db = null)
    {
        foreach ($this->createCommand($db)->queryBatchInternal($batchSize) as $rows) {
            foreach ($this->populate($rows) as $key => $model) {
                yield $key => $model;
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function batch($batchSize = 100, $db = null)
    {
        foreach ($this->createCommand($db)->queryBatchInternal($batchSize) as $rows) {
            yield $this->populate($rows);
        }
    }

}