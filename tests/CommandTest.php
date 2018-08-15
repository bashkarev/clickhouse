<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Query;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class CommandTest extends DatabaseTestCase
{

    public function testExecute()
    {
        $db = $this->getConnection();
        $sql = 'INSERT INTO {{customer}}([[id]], [[email]], [[name]], [[address]]) VALUES (4,\'user4@example.com\', \'user4\', \'address4\')';
        $command = $db->createCommand($sql);
        $this->assertEquals(1, $command->execute());
        $sql = 'SELECT COUNT(*) FROM {{customer}} WHERE [[name]] = \'user4\'';
        $command = $db->createCommand($sql);
        $this->assertEquals(1, $command->queryScalar());
        $command = $db->createCommand('bad SQL');
        $this->expectException(Exception::class);
        $command->execute();
    }

    public function testQuery()
    {
        $db = $this->getConnection();
        // query
        $sql = 'SELECT * FROM {{customer}}';
        $reader = $db->createCommand($sql)->query();
        $this->assertInstanceOf(\Generator::class, $reader);

        // queryAll
        $rows = $db->createCommand('SELECT * FROM {{customer}} ORDER BY [[id]]')->queryAll();
        $this->assertCount(3, $rows);
        $row = $rows[2];
        $this->assertEquals(3, $row['id']);
        $this->assertEquals('user3', $row['name']);
        $rows = $db->createCommand('SELECT * FROM {{customer}} WHERE [[id]] = 10')->queryAll();
        $this->assertEquals([], $rows);
        // queryOne
        $sql = 'SELECT * FROM {{customer}} ORDER BY [[id]]';
        $row = $db->createCommand($sql)->queryOne();
        $this->assertEquals(1, $row['id']);
        $this->assertEquals('user1', $row['name']);
        $sql = 'SELECT * FROM {{customer}} ORDER BY [[id]]';
        $command = $db->createCommand($sql);
        $row = $command->queryOne();
        $this->assertEquals(1, $row['id']);
        $this->assertEquals('user1', $row['name']);
        $sql = 'SELECT * FROM {{customer}} WHERE [[id]] = 10';
        $command = $db->createCommand($sql);
        $this->assertFalse($command->queryOne());
        // queryColumn
        $sql = 'SELECT * FROM {{customer}} ORDER BY [[id]]';
        $column = $db->createCommand($sql)->queryColumn();
        $this->assertEquals(range(1, 3), $column);
        $command = $db->createCommand('SELECT [[id]] FROM {{customer}} WHERE [[id]] = 10');
        $this->assertEquals([], $command->queryColumn());
        // queryScalar
        $sql = 'SELECT * FROM {{customer}} ORDER BY [[id]]';
        $this->assertEquals($db->createCommand($sql)->queryScalar(), 1);
        $sql = 'SELECT [[id]] FROM {{customer}} ORDER BY [[id]]';
        $command = $db->createCommand($sql);
        $this->assertEquals(1, $command->queryScalar());
        $command = $db->createCommand('SELECT [[id]] FROM {{customer}} WHERE [[id]] = 10');
        $this->assertFalse($command->queryScalar());
        $command = $db->createCommand('bad SQL');
        $this->expectException(Exception::class);
        $command->query();
    }

    public function testAddColumn()
    {
        $sql = $this->getConnection(false, false)->createCommand()->addColumn('user', 'dt', 'DateTime')->sql;
        $this->assertContains('ALTER TABLE `user` ADD COLUMN `dt` DateTime', $sql);
    }


    public function testQueryBatchInternal()
    {
        $db = $this->getConnection();
        $data = iterator_to_array((new Query())->from('customer')->each(1, $db), false);
        $this->assertCount(3, $data);
    }
}