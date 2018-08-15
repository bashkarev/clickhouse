<?php

namespace bashkarev\clickhouse;

use yii\db\ColumnSchemaBuilder as AbstractColumnSchemaBuilder;

/**
 * ColumnSchemaBuilder is the schema builder for ClickHouse databases.
 *
 * @author Sartor <sartorua@gmail.com>
 */
class ColumnSchemaBuilder extends AbstractColumnSchemaBuilder
{
    /**
     * @var bool whether the column is or not nullable.
     * If this is `false`, a `Nullable` type wrapper will be added.
     */
    protected $isNotNull = true;

    /**
     * {@inheritdoc}
     */
    public function defaultValue($default)
    {
        $this->default = $default;
        return $this;
    }

    protected function buildNotNullString()
    {
        if ($this->isNotNull !== true) {
            return ' NULL';
        }

        return '';
    }

    /**
     * {@inheritdoc}
     */
    protected function buildUnsignedString()
    {
        return $this->isUnsigned ? 'U' : '';
    }

    /**
     * {@inheritdoc}
     */
    protected function buildAfterString()
    {
        return $this->after !== null ?
            ' AFTER ' . $this->db->quoteColumnName($this->after) :
            '';
    }

    /**
     * {@inheritdoc}
     */
    protected function buildFirstString()
    {
        return $this->isFirst ? ' FIRST' : '';
    }

    protected function buildDefaultString()
    {
        $result = parent::buildDefaultString();

        if ($this->default === null && $this->isNotNull === false) {
            return '';
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function __toString()
    {
        switch ($this->getTypeCategory()) {
            case self::CATEGORY_NUMERIC:
                $format = "{unsigned}{type}{notnull}{default}{append}{pos}";
                break;
            default:
                $format = "{type}{length}{notnull}{default}{check}{append}{pos}";
        }

        return $this->buildCompleteString($format);
    }
}
