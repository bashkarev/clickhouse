<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use Yii;
use bashkarev\clickhouse\Parser;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class ParserTest extends TestCase
{

    public function testEmpty()
    {
        $this->assertEmpty(iterator_to_array($this->getParserGenerator('empty.txt')));
    }

    public function testValid()
    {
        $this->assertEquals([['123' => 123]], iterator_to_array($this->getParserGenerator('one.txt')));
        $this->assertEquals([['COUNT()' => '2000']], iterator_to_array($this->getParserGenerator('count.txt')));
        $this->assertCount(3, $this->getParserGenerator('ids.txt'));
        $this->assertCount(3, $this->getParserGenerator('customers.txt'));
    }

    public function testError()
    {
        $this->expectException(Exception::class);
        iterator_to_array($this->getParserGenerator('error.txt'));
    }

    /**
     * @param string $file
     * @param bool $forRead
     * @return \Generator
     */
    protected function getParserGenerator($file, $forRead = true)
    {
        return (new Parser($forRead))->run(fopen(Yii::getAlias("@data/parser/$file"), 'rb'));
    }


}