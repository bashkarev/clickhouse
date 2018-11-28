<?php

namespace bashkarev\clickhouse;

use yii\db\Expression;

/**
 * Class ColumnSchema for MySQL database
 *
 * @author Sartor <sartorua@gmail.com>
 */
class ColumnSchema extends \yii\db\ColumnSchema
{
    /**
     * {@inheritdoc}
     */
    public function dbTypecast($value)
    {
        if ($this->unsigned && in_array($this->type, [Schema::TYPE_BIGINT, Schema::TYPE_INTEGER], true) && is_numeric($value)) {
            $value = new Expression($value);
        }

        return parent::dbTypecast($value);
    }
}
