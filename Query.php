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
class Query extends \yii\db\Query
{
    /**
     * @inheritdoc
     */
    public function each($batchSize = 100, $db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('clickhouse');
        }
        foreach ($this->createCommand($db)->queryBatchInternal($batchSize) as $rows) {
            foreach ($this->populate($rows) as $key => $model) {
                yield $key => $model;
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function createCommand($db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('clickhouse');
        }
        return parent::createCommand($db);
    }

    /**
     * @inheritdoc
     */
    public function batch($batchSize = 100, $db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('clickhouse');
        }
        foreach ($this->createCommand($db)->queryBatchInternal($batchSize) as $rows) {
            yield $this->populate($rows);
        }
    }

    /**
     * @return $this
     */
    public function onFinal()
    {
        $this->params(['_final' => true]);
        return $this;
    }
}