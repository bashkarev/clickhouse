<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use yii\db\SchemaBuilderTrait;

/**
 * @author Sartor <sartorua@gmail.com>
 */
class SchemaBuilderTest extends DatabaseTestCase
{
    use SchemaBuilderTrait;

    private $tableName;

    protected function getDb()
    {
        return $this->getConnection();
    }

    protected function setUp()
    {
        parent::setUp();

        $this->tableName = uniqid('tmp_schema_test');
    }

    protected function tearDown()
    {
        $this->dropTable();

        parent::tearDown();
    }

    private function dropTable()
    {
        $sql = "DROP TABLE IF EXISTS `{$this->tableName}`";
        $this->getConnection()->createCommand($sql)->execute();
    }

    private function tableColumnsTypes()
    {
        $sql = "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '{$this->tableName}' ORDER BY name";

        return $this->getConnection()->createCommand($sql)->queryColumn();
    }

    private function tableColumnsDefaults()
    {
        $sql = "SELECT default_kind, default_expression FROM system.columns WHERE database = currentDatabase() AND table = '{$this->tableName}' ORDER BY name";

        return $this->getConnection()->createCommand($sql)->queryAll();
    }

    public function testIntegers()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'i1' => $this->tinyInteger(),
            'i2' => $this->tinyInteger()->unsigned(),
            'i3' => $this->smallInteger(),
            'i4' => $this->smallInteger()->unsigned(),
            'i5' => $this->integer(),
            'i6' => $this->integer()->unsigned(),
            'i7' => $this->bigInteger(),
            'i8' => $this->bigInteger()->unsigned(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals(['Int8', 'UInt8', 'Int16', 'UInt16', 'Int32', 'UInt32', 'Int64', 'UInt64'], $this->tableColumnsTypes());
    }

    public function testFloats()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'f1' => $this->float(),
            'f2' => $this->double(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals(['Float32', 'Float64'], $this->tableColumnsTypes());
    }

    public function testString()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            's1' => $this->string(),
            's2' => $this->text(),
            's3' => $this->binary(),
            's4' => $this->string(1000000),
            's5' => $this->char(),
            's6' => $this->char(100),
            's7' => $this->json(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals(['String', 'String', 'String', 'String', 'FixedString(1)', 'FixedString(100)', 'String'], $this->tableColumnsTypes());
    }

    public function testDate()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'd1' => $this->date(),
            'd2' => $this->dateTime(),
            'd3' => $this->dateTime(0),
            'd4' => $this->dateTime(6),
            'd5' => $this->timestamp(),
            'd6' => $this->timestamp(6)
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals(['Date', 'DateTime', 'DateTime', 'DateTime', 'DateTime', 'DateTime'], $this->tableColumnsTypes());
    }

    public function testBoolean()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'b1' => $this->boolean(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals(['UInt8'], $this->tableColumnsTypes());
    }

    public function testFixed()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'f1' => $this->decimal(9, 0),
            'f2' => $this->decimal(9, 2),
            'f3' => $this->decimal(9, 9),
            'f4' => $this->decimal(38, 2),

            'f5' => $this->money(9, 2),
            'f6' => $this->money(38, 6),

            'f7' => $this->decimal(),
            'f8' => $this->money(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $this->assertEquals([
            'Decimal(9, 0)', 'Decimal(9, 2)', 'Decimal(9, 9)', 'Decimal(38, 2)',
            'Decimal(9, 2)', 'Decimal(38, 6)',
            'Decimal(9, 2)', 'Decimal(18, 2)'
        ], $this->tableColumnsTypes());
    }

    public function testNullable()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'n01' => $this->integer()->null(),
            'n02' => $this->integer()->null()->unsigned(),
            'n03' => $this->float()->null(),
            'n04' => $this->double()->null(),
            'n05' => $this->string()->null(),
            'n06' => $this->boolean()->null(),
            'n07' => $this->date()->null(),
            'n08' => $this->dateTime()->null(),
            'n09' => $this->decimal()->null(),
            'n10' => $this->money()->null(),
            'n11' => $this->char(100)->null(),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $expected = [
            'Nullable(Int32)', 'Nullable(UInt32)', 'Nullable(Float32)', 'Nullable(Float64)',
            'Nullable(String)', 'Nullable(UInt8)', 'Nullable(Date)', 'Nullable(DateTime)',
            'Nullable(Decimal(9, 2))', 'Nullable(Decimal(18, 2))', 'Nullable(FixedString(100))'
        ];

        $this->assertEquals($expected, $this->tableColumnsTypes());
    }

    public function testDefaultExpression()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'd1' => $this->integer()->defaultExpression('100'),
            'd2' => $this->integer()->null()->unsigned()->defaultExpression('100'),
            'd3' => $this->char(10)->null()->defaultExpression("'qweasdzxc1'"),
            'd4' => $this->string()->defaultExpression("'str1'"),
            'd5' => $this->string()->defaultExpression("concat('str1', 'str2')"),
            'd6' => $this->date()->defaultExpression("today()"),
            'd7' => $this->dateTime()->defaultExpression("now()"),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $expected = [
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST(100, 'Int32')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST(100, 'Nullable(UInt32)')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST('qweasdzxc1', 'Nullable(FixedString(10))')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "'str1'"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "concat('str1', 'str2')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "today()"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "now()"],
        ];

        $this->assertEquals($expected, $this->tableColumnsDefaults());
    }

    public function testDefaultValue()
    {
        $this->dropTable();

        $db = $this->getConnection();

        $createResult = $db->createCommand()->createTable($this->tableName, [
            'd1' => $this->integer()->defaultValue('100'),
            'd2' => $this->integer()->defaultValue(100),
            'd3' => $this->integer()->null()->unsigned()->defaultValue(100),
            'd4' => $this->char(10)->null()->defaultValue('qweasdzxc1'),
            'd5' => $this->string()->defaultValue('str1'),
        ], 'Engine=Memory')->execute();

        $this->assertEquals(1, $createResult);

        $expected = [
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST('100', 'Int32')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST(100, 'Int32')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST(100, 'Nullable(UInt32)')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "CAST('qweasdzxc1', 'Nullable(FixedString(10))')"],
            ['default_kind' => 'DEFAULT', 'default_expression' => "'str1'"],
        ];

        $this->assertEquals($expected, $this->tableColumnsDefaults());
    }
}