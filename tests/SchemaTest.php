<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/PhpStorm/blob/master/LICENSE
 * @link https://github.com/bashkarev/PhpStorm#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Schema;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class SchemaTest extends DatabaseTestCase
{

    public function testTypes()
    {
        $columns = $this->getConnection()->getSchema()->getTableSchema('types', true)->columns;

        $this->assertSame(Schema::TYPE_SMALLINT, $columns['UInt8']->type);
        $this->assertSame(Schema::TYPE_INTEGER, $columns['UInt16']->type);
        $this->assertSame(Schema::TYPE_INTEGER, $columns['UInt32']->type);
        $this->assertSame(Schema::TYPE_BIGINT, $columns['UInt64']->type);

        $this->assertSame(Schema::TYPE_SMALLINT, $columns['Int8']->type);
        $this->assertSame(Schema::TYPE_INTEGER, $columns['Int16']->type);
        $this->assertSame(Schema::TYPE_INTEGER, $columns['Int32']->type);
        $this->assertSame(Schema::TYPE_BIGINT, $columns['Int64']->type);

        $this->assertSame(Schema::TYPE_FLOAT, $columns['Float32']->type);
        $this->assertSame(Schema::TYPE_FLOAT, $columns['Float64']->type);

        $this->assertSame(Schema::TYPE_STRING, $columns['String']->type);
        $this->assertSame(Schema::TYPE_STRING, $columns['FixedString']->type);

        $this->assertSame(Schema::TYPE_DATETIME, $columns['DateTime']->type);
        $this->assertSame(Schema::TYPE_DATE, $columns['Date']->type);

        $this->assertSame(Schema::TYPE_STRING, $columns['Enum8']->type);
        $this->assertSame(Schema::TYPE_STRING, $columns['Enum16']->type);

    }

    public function testSize()
    {
        $column = $this->getConnection()->getSchema()->getTableSchema('types', true)->columns['FixedString'];
        $this->assertSame(20, $column->size);
    }

}