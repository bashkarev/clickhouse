<?php

namespace bashkarev\clickhouse;

use yii\db\ArrayExpression;
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

        if (strpos($this->dbType, 'Array(') === 0) {
            return new ArrayExpression($value, $this->type);
        }

        return parent::dbTypecast($value);
    }
}
