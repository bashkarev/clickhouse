<?php

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Query;

class QueryTest extends DatabaseTestCase
{
    public function testScalar()
    {
        $db = $this->getConnection();
        $name = (new Query())
            ->select('name')
            ->from('{{customer}}')
            ->where([
                'id' => 1
            ])
            ->scalar($db);

        $this->assertEquals('user1', $name);
    }

    public function testOne()
    {
        $db = $this->getConnection();
        $one = (new Query())
            ->select(['name', 'address'])
            ->from('{{customer}}')
            ->where([
                'id' => 1
            ])
            ->one($db);

        $this->assertEquals(['name' => 'user1', 'address' => 'address1'], $one);
    }

    public function testAll()
    {
        $db = $this->getConnection();
        $rows = (new Query())
            ->select(['name', 'address'])
            ->from('{{customer}}')
            ->where([
                'id' => [1, 2]
            ])
            ->orderBy(['id' => SORT_ASC])
            ->all($db);

        $this->assertEquals([
            ['name' => 'user1', 'address' => 'address1'],
            ['name' => 'user2', 'address' => 'address2']
        ], $rows);
    }

    public function testLimitOffset()
    {
        $db = $this->getConnection();
        $rows = (new Query())
            ->select('name')
            ->from('{{customer}}')
            ->where([
                'id' => [1, 2]
            ])
            ->orderBy(['id' => SORT_ASC])
            ->limit(1)
            ->offset(1)
            ->all($db);

        $this->assertEquals([['name' => 'user2']], $rows);
    }

    public function testGroupBy()
    {
        $db = $this->getConnection();
        $rows = (new Query())
            ->select([
                'status',
                'count' => 'COUNT(*)'
            ])
            ->from('{{customer}}')
            ->where([
                'id' => [1, 2, 3]
            ])
            ->groupBy('status')
            ->orderBy(['status' => SORT_ASC])
            ->all($db);

        $this->assertEquals([
            ['status' => 1, 'count' => 2],
            ['status' => 2, 'count' => 1]
        ], $rows);
    }

    public function testSubQuery()
    {
        $db = $this->getConnection();
        $subQuery = (new Query())
            ->select('name')
            ->from('{{customer}}')
            ->where([
                'id' => [1]
            ]);

        $rows = (new Query())
            ->select('address')
            ->from('{{customer}}')
            ->where([
                'name' => $subQuery
            ])
            ->all($db);

        $this->assertEquals([
            ['address' => 'address1'],
        ], $rows);
    }

    public function testUnionAll()
    {
        $db = $this->getConnection();
        $query1 = (new Query())
            ->select('name')
            ->from('{{customer}}')
            ->where([
                'id' => [1]
            ]);

        $query2 = (new Query())
            ->select('name')
            ->from('{{customer}}')
            ->where([
                'id' => [2]
            ]);

        $rows = (new Query())
            ->select('name')
            ->from(['result' => $query1->union($query2, true)])
            ->orderBy(['name' => SORT_ASC])
            ->all($db);

        // Using Subquery, because of ClickHouse does not sort result

        $this->assertEquals([
            ['name' => 'user1'],
            ['name' => 'user2']
        ], $rows);
    }
}
