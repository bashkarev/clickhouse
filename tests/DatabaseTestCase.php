<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Connection;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
abstract class DatabaseTestCase extends TestCase
{
    /**
     * @var Connection
     */
    private $_db;

    protected function setUp()
    {
        $this->mockApplication();
    }

    /**
     * @param  bool $reset whether to clean up the test database
     * @param  bool $open whether to open and populate test database
     * @return Connection
     */
    public function getConnection($reset = true, $open = true)
    {
        if (!$reset && $this->_db) {
            return $this->_db;
        }
        $config = self::getParam('database');
        if (isset($config['fixture'])) {
            $fixture = $config['fixture'];
            unset($config['fixture']);
        } else {
            $fixture = null;
        }
        try {
            $this->_db = $this->prepareDatabase($config, $fixture, $open);
        } catch (\Exception $e) {
            $this->markTestSkipped("Something wrong when preparing database: " . $e->getMessage());
        }
        return $this->_db;
    }

    public function prepareDatabase($config, $fixture, $open = true)
    {
        /* @var $db Connection */
        $db = \Yii::createObject($config);
        if (!$open) {
            return $db;
        }
        $db->open();
        if ($fixture !== null) {
            $lines = explode(';', file_get_contents($fixture));
            foreach ($lines as $line) {
                if (trim($line) !== '') {
                    $db->executeCommand($line, false);
                }
            }
        }
        return $db;
    }

}