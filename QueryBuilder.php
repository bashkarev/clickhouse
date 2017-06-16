<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class QueryBuilder extends \yii\db\QueryBuilder
{

    /**
     * @inheritdoc
     */
    public function createTable($table, $columns, $options = null)
    {
        if ($options === null) {
            throw new Exception('Specify engine type');
        }
        return parent::createTable($table, $columns, $options);
    }

    /**
     * @inheritdoc
     */
    public function buildFrom($tables, &$params)
    {
        $final = false;
        if (isset($params['_final'])) {
            $final = $params['_final'];
            unset($params['_final']);
        }
        $from = parent::buildFrom($tables, $params);
        if ($final === true) {
            $from .= ' FINAL';
        }
        return $from;
    }

    /**
     * @inheritdoc
     */
    public function addColumn($table, $column, $type)
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' ADD COLUMN ' . $this->db->quoteColumnName($column) . ' '
            . $this->getColumnType($type);
    }
}