<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\db\ArrayExpression;
use yii\db\Exception;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class QueryBuilder extends \yii\db\QueryBuilder
{
    /**
     * @var array mapping from abstract column types (keys) to physical column types (values).
     */
    public $typeMap = [
        Schema::TYPE_CHAR => 'FixedString(1)',
        Schema::TYPE_STRING => 'String',
        Schema::TYPE_TEXT => 'String',
        Schema::TYPE_TINYINT => 'Int8',
        Schema::TYPE_SMALLINT => 'Int16',
        Schema::TYPE_INTEGER => 'Int32',
        Schema::TYPE_BIGINT => 'Int64',
        'U'.Schema::TYPE_TINYINT => 'UInt8',
        'U'.Schema::TYPE_SMALLINT => 'UInt16',
        'U'.Schema::TYPE_INTEGER => 'UInt32',
        'U'.Schema::TYPE_BIGINT => 'UInt64',
        Schema::TYPE_FLOAT => 'Float32',
        Schema::TYPE_DOUBLE => 'Float64',
        Schema::TYPE_DECIMAL => 'Decimal(9,2)',
        Schema::TYPE_DATETIME => 'DateTime',
        Schema::TYPE_TIMESTAMP => 'DateTime',
        Schema::TYPE_TIME => 'DateTime',
        Schema::TYPE_DATE => 'Date',
        Schema::TYPE_BINARY => 'String',
        Schema::TYPE_BOOLEAN => 'UInt8',
        Schema::TYPE_MONEY => 'Decimal(18,2)',
        Schema::TYPE_JSON => 'String'
    ];

    protected function defaultExpressionBuilders()
    {
        return array_merge(parent::defaultExpressionBuilders(), [
            ArrayExpression::class => ArrayExpressionBuilder::class,
        ]);
    }

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


    public function getColumnType($type)
    {
        // Replacing NULL to Nullable() wrapper
        return preg_replace('/^(\w+)(\(\d+(?>\s*,\s*\d+)?\))? NULL(.*)$/i', 'Nullable(\1\2)\3', parent::getColumnType($type));
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
