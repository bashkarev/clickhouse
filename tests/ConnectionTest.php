<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Connection;
use yii\base\InvalidConfigException;
use yii\base\NotSupportedException;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class ConnectionTest extends DatabaseTestCase
{

    public function testConstruct()
    {
        $connection = $this->getConnection(false);
        $params = self::getParam('database');

        $this->assertEquals($params['dsn'], $connection->dsn);
        $this->assertEquals($params['username'], $connection->username);
        $this->assertEquals($params['password'], $connection->password);
    }

    public function testOpenClose()
    {
        $connection = $this->getConnection(false, false);

        $this->assertFalse($connection->isActive);

        $connection->open();
        $this->assertTrue($connection->isActive);

        $connection->close();
        $this->assertFalse($connection->isActive);
    }

    public function testInvalidConfig()
    {
        $connection = $this->getConnection(false, false);

        $this->assertFalse($connection->isActive);

        $connection = new Connection();
        $connection->dsn = 'port=';
        $this->expectException(InvalidConfigException::class);
        $connection->open();
    }

    public function testSerialize()
    {
        $connection = $this->getConnection(false, false);
        $connection->open();
        $serialized = serialize($connection);
        $unserialized = unserialize($serialized);
        $this->assertInstanceOf(Connection::class, $unserialized);
        $this->assertEquals(123, $unserialized->createCommand("SELECT 123")->queryScalar());
    }

    public function testGetDriverName()
    {
        $connection = $this->getConnection(false, false);
        $this->assertEquals('clickhouse', $connection->driverName);
        $this->assertEquals('clickhouse', $connection->getDriverName());
    }

    public function testQuoteValue()
    {
        $connection = $this->getConnection(false);
        $this->assertEquals(123, $connection->quoteValue(123));
        $this->assertEquals("'string'", $connection->quoteValue('string'));
        $this->assertEquals("'It\\'s interesting'", $connection->quoteValue("It's interesting"));
    }

    public function testQuoteTableName()
    {
        $connection = $this->getConnection(false, false);
        $this->assertEquals('`table`', $connection->quoteTableName('table'));
        $this->assertEquals('`table`', $connection->quoteTableName('`table`'));
        $this->assertEquals('`schema`.`table`', $connection->quoteTableName('schema.table'));
        $this->assertEquals('`schema`.`table`', $connection->quoteTableName('schema.`table`'));
        $this->assertEquals('`schema`.`table`', $connection->quoteTableName('`schema`.`table`'));
        $this->assertEquals('{{table}}', $connection->quoteTableName('{{table}}'));
        $this->assertEquals('(table)', $connection->quoteTableName('(table)'));
    }

    public function testQuoteColumnName()
    {
        $connection = $this->getConnection(false, false);
        $this->assertEquals('`column`', $connection->quoteColumnName('column'));
        $this->assertEquals('`column`', $connection->quoteColumnName('`column`'));
        $this->assertEquals('[[column]]', $connection->quoteColumnName('[[column]]'));
        $this->assertEquals('{{column}}', $connection->quoteColumnName('{{column}}'));
        $this->assertEquals('(column)', $connection->quoteColumnName('(column)'));

        $this->assertEquals('`column`', $connection->quoteSql('[[column]]'));
        $this->assertEquals('`column`', $connection->quoteSql('{{column}}'));
    }

    public function testQuoteFullColumnName()
    {
        $connection = $this->getConnection(false, false);
        $this->assertEquals('`table`.`column`', $connection->quoteColumnName('table.column'));
        $this->assertEquals('`table`.`column`', $connection->quoteColumnName('table.`column`'));
        $this->assertEquals('`table`.`column`', $connection->quoteColumnName('`table`.column'));
        $this->assertEquals('`table`.`column`', $connection->quoteColumnName('`table`.`column`'));
        $this->assertEquals('[[table.column]]', $connection->quoteColumnName('[[table.column]]'));
        $this->assertEquals('{{table}}.`column`', $connection->quoteColumnName('{{table}}.column'));
        $this->assertEquals('{{table}}.`column`', $connection->quoteColumnName('{{table}}.`column`'));
        $this->assertEquals('{{table}}.[[column]]', $connection->quoteColumnName('{{table}}.[[column]]'));
        $this->assertEquals('{{%table}}.`column`', $connection->quoteColumnName('{{%table}}.column'));
        $this->assertEquals('{{%table}}.`column`', $connection->quoteColumnName('{{%table}}.`column`'));
        $this->assertEquals('`table`.`column`', $connection->quoteSql('[[table.column]]'));
        $this->assertEquals('`table`.`column`', $connection->quoteSql('{{table}}.[[column]]'));
        $this->assertEquals('`table`.`column`', $connection->quoteSql('{{table}}.`column`'));
        $this->assertEquals('`table`.`column`', $connection->quoteSql('{{%table}}.[[column]]'));
        $this->assertEquals('`table`.`column`', $connection->quoteSql('{{%table}}.`column`'));
    }

    public function testTransaction()
    {
        $this->expectException(NotSupportedException::class);
        $this->getConnection()->beginTransaction();
    }
}