<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use Yii;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class InsertFiles
{
    /**
     * @var int Maximum chunk size.
     */
    public $chunkSize = 4096;
    /**
     * @var Connection
     */
    protected $db;
    /**
     * @var string
     */
    protected $url;
    /**
     * @var string
     */
    protected $sql;
    /**
     * @var array
     */
    protected $files = [];

    /**
     * InsertFiles constructor.
     * @param Connection $db
     * @param string $table
     * @param array $files
     * @param array $columns
     */
    public function __construct(Connection $db, $table, $files = [], $columns = [])
    {
        $this->db = $db;
        $this->prepare($table, $columns);
        if ($files !== []) {
            $this->setFiles($files);
        }
    }

    /**
     * @param mixed $files
     * @return $this
     * @throws \Exception
     */
    public function setFiles($files)
    {
        if (!is_array($files)) {
            $files = (array)$files;
        }
        foreach ($files as $file) {
            $file = \Yii::getAlias($file);
            if (file_exists($file) === false) {
                throw new FileNotFoundException("File: `{$file}` not found");
            }
            $this->files[] = $file;
        }
        return $this;
    }

    /**
     * @return array
     */
    public function getFiles()
    {
        return $this->files;
    }

    /**
     * @param string $table
     * @param array $columns
     */
    protected function prepare($table, $columns)
    {
        $this->sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table);
        if ($columns !== []) {
            $this->sql .= ' (' . implode(', ', $columns) . ')';
        }
        $this->sql .= ' FORMAT CSV';
        $this->url = $this->db->getConfiguration()->prepareUrl(['query' => $this->sql]);
    }

    /**
     * @param $file
     * @return \Generator
     */
    protected function runItem($file)
    {
        $token = $this->sql . ' `' . basename($file) . '`';

        Yii::info($token, 'bashkarev\clickhouse\Command::query');
        Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');
        $handle = fopen($file, 'rb');
        $generator = $this->db->execute();
        $generator->send("POST {$this->url} HTTP/1.1\r\n");
        $generator->send("Transfer-Encoding: chunked\r\n\r\n");

        while (true) {
            $data = fread($handle, $this->chunkSize);
            if ($data === false || ($length = strlen($data)) === 0) {
                $generator->send("0\r\n\r\n");
                $generator->send(false);
                break 1;
            }
            $generator->send(dechex($length) . "\r\n");
            $generator->send($data . "\r\n");
            yield;
        }
        fclose($handle);

        while ($generator->valid()) {
            $generator->next();
            yield;
        }
        Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
    }

    /**
     * Execute
     * @throws Exception
     */
    public function execute()
    {
        $queue = new \SplQueue();
        foreach ($this->files as $file) {
            $queue->enqueue($this->runItem($file));
        }

        while (!$queue->isEmpty()) {
            $task = $queue->dequeue();
            $task->next();
            if ($task->valid()) {
                $queue->enqueue($task);
            }
        }
        $this->files = [];
    }


}