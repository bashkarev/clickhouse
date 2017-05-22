<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests\helpers;

use bashkarev\clickhouse\tests\TestCase;

use bashkarev\clickhouse\helpers\Csv;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class CsvTest extends TestCase
{

    public function testNull()
    {
        $this->assertEquals('', Csv::toString([null]));
    }

    public function testInt()
    {
        $this->assertEquals('1,2,3', Csv::toString([1, 2, 3]));
    }

    public function testFloat()
    {
        $this->assertEquals('1.22', Csv::toString([1.22]));
        setlocale(LC_ALL, 'de_DE.UTF-8');
        $this->assertEquals('1.22', Csv::toString([1.22]));
        setlocale(LC_ALL, 'en_US.UTF-8');
    }

    public function testString()
    {
        $this->assertEquals('2017-04-04', Csv::toString(['2017-04-04']));
        $this->assertEquals('2017-04-04 10:56:16', Csv::toString(['2017-04-04 10:56:16']));
        $this->assertEquals('"""test"', Csv::toString(['"test']));
        $this->assertEquals('"""test"""', Csv::toString(['"test"']));
    }

    public function testEmptyString()
    {
        $this->assertEquals(',,', Csv::toString(['', '', '']));
    }

    /**
     * toDo
     */
    public function testArray()
    {
        //$this->assertEquals('[222]', Csv::toString([[222]]));
    }

}