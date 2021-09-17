<?php

namespace bashkarev\clickhouse;

/**
 * Schema builder trait
 * 
 * Provides some shortcuts for specific clickhouse data types and etc.
 * 
 * @see \yii\db\SchemaBuilderTrait For better understanding about logic of this trait
 * @method \yii\db\Connection getDb()
 */
trait SchemaBuilderTrait
{
    /**
     * Creates an UUID column
     *
     * @return ColumnSchemaBuilder
     * @see https://clickhouse.tech/docs/en/sql-reference/data-types/uuid/ For more information about UUID columns
     */
    public function uuid(): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_UUID);
    }
}
