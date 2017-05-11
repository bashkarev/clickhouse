<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse\tests;

use yii\db\Exception;
use bashkarev\clickhouse\FileNotFoundException;
use bashkarev\clickhouse\InsertFiles;
use bashkarev\clickhouse\Query;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class InsertFilesTest extends DatabaseTestCase
{

    public function testFileAlias()
    {
        \Yii::setAlias('@InsertFilesTest', __DIR__);
        $this->assertEquals([__FILE__], $this->getInsertFiles()->setFiles(['@InsertFilesTest/InsertFilesTest.php'])->getFiles());
    }

    public function testFileNotFound()
    {
        $this->expectException(FileNotFoundException::class);
        $this->getInsertFiles()->setFiles(__DIR__ . '/FileNotFound');
    }

    public function testExecute()
    {
        $this->getInsertFiles()->setFiles([
            '@data/csv/e1e747f9901e67ca121768b36921fbae.csv',
            '@data/csv/ebe191dfc36d73aece91e92007d24e3e.csv',
            '@data/csv/empty.csv'
        ])->execute();
        $count = (new Query)->from('csv')->count('*', $this->getConnection(false, false));
        $this->assertContains('2000', $count);
    }

    public function testInvalidFile()
    {
        $this->expectException(Exception::class);
        $this->getInsertFiles()->setFiles('@data/csv/not_valid.csv')->execute();
    }

    /**
     * @return InsertFiles
     */
    protected function getInsertFiles()
    {
        return new InsertFiles($this->getConnection(false, false), 'csv');
    }

}