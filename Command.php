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
        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');

            $result = iterator_to_array($this->db->executeCommand($rawSql), false);

            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'bashkarev\clickhouse\Command::query');
            throw $e;
        }
        if ($method == 'fetch') {
            return array_shift($result);
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

            iterator_to_array($this->db->executeCommand($rawSql, false), false);

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
        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'bashkarev\clickhouse\Command::query');
            $index = 0;
            foreach ($this->db->executeCommand($rawSql) as $row) {
                $count++;
                $rows[$index] = $row;
                if ($count >= $size) {
                    yield $rows;
                    $rows = [];
                    $count = 0;
                }
                ++$index;
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

}