<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use Yii;

/**
 * @property Connection $db
 *
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Command extends \yii\db\Command
{
    /**
     * @inheritdoc
     */
    protected function queryInternal($method, $fetchMode = null)
    {
        $rawSql = $this->getRawSql();

        Yii::info($rawSql, 'bashkarev\clickhouse\Command::query');
        if ($method !== '') {
            $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->queryCacheDependency);
            if (is_array($info)) {
                /* @var $cache \yii\caching\Cache */
                $cache = $info[0];
                $cacheKey = [
                    __CLASS__,
                    $method,
                    $fetchMode,
                    $this->db->dsn,
                    $this->db->username,
                    $rawSql,
                ];
                $result = $cache->get($cacheKey);
                if (is_array($result) && isset($result[0])) {
                    Yii::trace('Query result served from cache', 'bashkarev\clickhouse\Command::query');
                    return $result[0];
                }
            }
        }
        $generator = $this->db->execute();
        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');
            $generator->send($this->createRequest($rawSql, true));
            $generator->send(false);
            if ($method === '') {
                return $generator;
            }
            $result = call_user_func_array([$this, $method], [$generator, $fetchMode]);
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
            throw $e;
        }

        if (isset($cache, $cacheKey, $info)) {
            $cache->set($cacheKey, [$result], $info[1], $info[2]);
            Yii::trace('Saved query result in cache', 'bashkarev\clickhouse\Command::query');
        }
        return $result;
    }

    /**
     * @inheritdoc
     */
    public function execute()
    {
        $rawSql = $this->getRawSql();

        Yii::info($rawSql, __METHOD__);

        if ($this->sql == '') {
            return 0;
        }
        $generator = $this->db->execute();
        $token = $rawSql;
        try {
            Yii::beginProfile($token, __METHOD__);
            $generator->send($this->createRequest($rawSql, false));
            $generator->send(false);
            while ($generator->valid()) {
                $generator->next();
            }
            Yii::endProfile($token, __METHOD__);
            $this->refreshTableSchema();
            return 1;
        } catch (\Exception $e) {
            Yii::endProfile($token, __METHOD__);
            throw $e;
        }
    }

    /**
     * @param int $size
     * @return \Generator
     * @throws \Exception
     */
    public function queryBatchInternal($size)
    {
        $rawSql = $this->getRawSql();
        $count = 0;
        $rows = [];
        Yii::info($rawSql, 'bashkarev\clickhouse\Command::query');
        $generator = $this->db->execute();
        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');
            $index = 0;
            while ($generator->valid()) {
                $count++;
                $rows[$index] = $generator->current();
                if ($count >= $size) {
                    yield $rows;
                    $rows = [];
                    $count = 0;
                }
                ++$index;
                $generator->next();
            }
            if ($rows !== []) {
                yield $rows;
            }
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
            throw $e;
        }
    }

    /**
     * @param string $table
     * @param array $files
     * @param array $columns
     * @return InsertFiles
     */
    public function batchInsertFiles($table, $files = [], $columns = [])
    {
        return new InsertFiles($this->db, $table, $files, $columns);
    }

    /**
     * @param string $sql
     * @param bool $forRead
     * @return string
     */
    protected function createRequest($sql, $forRead)
    {
        $data = $sql;
        $url = $this->db->getConfiguration()->prepareUrl();
        if ($forRead === true) {
            $data .= ' FORMAT JSONEachRow';
        }
        $header = "POST $url HTTP/1.1\r\n";
        $header .= "Content-Length: " . strlen($data) . "\r\n";
        $header .= "\r\n";
        $header .= $data;

        return $header;
    }

    /**
     * @param \Generator $generator
     * @param int $mode
     * @return array
     */
    protected function fetchAll(\Generator $generator, $mode)
    {
        $result = [];
        if ($mode === \PDO::FETCH_COLUMN) {
            while ($generator->valid()) {
                $result[] = current($generator->current());
                $generator->next();
            }
        } else {
            while ($generator->valid()) {
                $result[] = $generator->current();
                $generator->next();
            }
        }
        return $result;
    }

    /**
     * @param \Generator $generator
     * @param $mode
     * @return bool|mixed
     */
    protected function fetchColumn(\Generator $generator, $mode)
    {
        if (!$generator->valid()) {
            return false;
        }
        return current($generator->current());
    }

    /**
     * @param \Generator $generator
     * @param $mode
     * @return bool|mixed
     */
    protected function fetch(\Generator $generator, $mode)
    {
        if (!$generator->valid()) {
            return false;
        }
        return $generator->current();
    }

}