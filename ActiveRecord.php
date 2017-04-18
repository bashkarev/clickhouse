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
abstract class ActiveRecord extends \yii\db\ActiveRecord
{

    /**
     * @inheritdoc
     */
    public static function getDb()
    {
        return Yii::$app->get('clickhouse');
    }

    /**
     * @return ActiveQuery
     */
    public static function find()
    {
        return new ActiveQuery(get_called_class());
    }

}