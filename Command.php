<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use ClickHouseDB\Exception\QueryException;
use ClickHouseDB\Statement;
use Yii;
use yii\db\Exception;

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

        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');
            $statement = $this->db->executeSelect($rawSql);
            if ($method === '') {
                return $statement->rows();
            }
            $result = call_user_func_array([$this, $method], [$statement, $fetchMode]);
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
        } catch (QueryException $e) {
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
            throw new Exception($e->getMessage());
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

        $token = $rawSql;
        try {
            Yii::beginProfile($token, __METHOD__);
            $statement = $this->db->execute($rawSql);
            $this->refreshTableSchema();
            return (int)(!$statement->isError());
        } catch (QueryException $e) {
            Yii::endProfile($token, __METHOD__);
            throw new Exception($e->getMessage());
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
        // TODO: real batch select
        $allRows = $this->queryAll();

        $count = 0;
        $index = 0;
        $rows = [];
        foreach ($allRows as $row) {
            $count++;
            $rows[$index] = $row;
            if ($count >= $size) {
                yield $rows;
                $rows = [];
                $count = 0;
            }
            $index++;
        }

        if ($rows !== []) {
            yield $rows;
        }
    }

    /**
     * @param \ClickHouseDB\Statement $statement
     * @param int $mode
     * @return array
     */
    protected function fetchAll(Statement $statement, $mode)
    {
        $result = $statement->rows();

        if ($result === null) {
            return [];
        }

        if ($mode !== \PDO::FETCH_COLUMN) {
            return $result;
        }

        $firstRow = current($result);
        if ($firstRow === false) {
            return [];
        }

        $firstKey = current(array_keys($firstRow));

        return array_column($result, $firstKey);

    }

    /**
     * @param \ClickHouseDB\Statement $statement
     * @param $mode
     * @return bool|mixed
     */
    protected function fetchColumn(Statement $statement, $mode)
    {
        $row = $statement->fetchOne();

        if ($row === null) {
            return false;
        }

        return current($row);
    }

    /**
     * @param \ClickHouseDB\Statement $statement
     * @param $mode
     * @return bool|mixed
     */
    protected function fetch(Statement $statement, $mode)
    {
        return $statement->fetchOne()??false;
    }
}