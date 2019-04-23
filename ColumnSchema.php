<?php

namespace bashkarev\clickhouse;

use yii\db\Expression;

/**
 * Class ColumnSchema for ClickHouse database
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
        if ($this->unsigned && $this->type === Schema::TYPE_BIGINT && is_numeric($value)) {
            $value = new Expression($value);
        }

        return parent::dbTypecast($value);
    }
}
