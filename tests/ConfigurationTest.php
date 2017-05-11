<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Configuration;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class ConfigurationTest extends DatabaseTestCase
{

    public function testConstruct()
    {
        $config = new Configuration('', '', '');
        $this->assertEquals('tcp://127.0.0.1:8123', $config->getAddress());
        $this->assertEquals('/', $config->prepareUrl());
    }

    public function testUnknownSetting()
    {
        $config = self::getParam('database');
        $config['dsn'] = 'unknown-setting=test';
        unset($config['fixture']);
        $connection = $this->prepareDatabase($config, null, false);
        $this->expectException(Exception::class);
        $connection->createCommand('SELECT 123')->queryScalar();
    }

    public function testAddress()
    {
        $this->assertEquals('tcp://localhost:1010', (new Configuration('host=localhost;port=1010', '', ''))->getAddress());
        $this->assertEquals('tcp://127.0.0.1:1011', (new Configuration('port=1011', '', ''))->getAddress());
        $this->assertEquals('tcp://[2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d]:8123', (new Configuration('host=[2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d]', '', ''))->getAddress());
        $this->assertEquals('tcp://0.0.0.0:8123', (new Configuration('host=0.0.0.0', '', ''))->getAddress());
    }

    public function testPrepareUrl()
    {
        $this->assertEquals('/?ya=ya', (new Configuration('ya=ya', '', ''))->prepareUrl());
        $this->assertEquals('/?user=ya&password=ya', (new Configuration('', 'ya', 'ya'))->prepareUrl());

        $this->assertEquals('/?user=ya&password=ya&temp=te+mp', (new Configuration('', 'ya', 'ya'))->prepareUrl(['temp' => 'te mp']));
        $this->assertEquals('/?ya=ya&temp=te+mp', (new Configuration('', '', ''))->prepareUrl(['ya' => 'ya', 'temp' => 'te mp']));

    }

    public function testChangeUserOrPassword()
    {
        $this->assertEquals('/?user=ya&password=ya', (new Configuration('user=ttt;password=ttt', 'ya', 'ya'))->prepareUrl());

        $this->expectException(\LogicException::class);
        (new Configuration('', 'ya', 'ya'))->prepareUrl(['user' => 'ttt', 'password' => 'ttt']);
    }

}