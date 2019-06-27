<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Connection;
use bashkarev\clickhouse\Query;
use ClickHouseDB\Statement;
use yii\base\InvalidConfigException;
use yii\base\NotSupportedException;

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

    public function testAutoOpen()
    {
        $connection = $this->getConnection(false, false);

        $this->assertFalse($connection->isActive);

        $connection->execute("SELECT 1");
        $this->assertTrue($connection->isActive);

        $connection->close();
        $this->assertFalse($connection->isActive);

        $connection->executeSelect("SELECT 1");
        $this->assertTrue($connection->isActive);
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

    public function testBatchInsert()
    {
        $connection = $this->getConnection();
        $client = $connection->getClient();

        $files = [
            \Yii::getAlias('@data/csv/e1e747f9901e67ca121768b36921fbae.csv'),
            \Yii::getAlias('@data/csv/ebe191dfc36d73aece91e92007d24e3e.csv'),
            \Yii::getAlias('@data/csv/empty.csv'),
        ];

        $result = $client->insertBatchFiles('csv', $files);
        $count = (new Query)
            ->from('csv')
            ->count('*', $connection);

        foreach ($result as $filename => $state) {
            /** @var Statement $state */
            $this->assertEquals($state->isError(), false);
        }

        $this->assertContains('2000', $count);
    }
}