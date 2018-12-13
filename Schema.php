<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\db\TableSchema;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Schema extends \yii\db\mysql\Schema
{
    public $columnSchemaClass = 'bashkarev\clickhouse\ColumnSchema';

    /**
     * toDo Array(T), Tuple(T1, T2, ...), Nested
     * @var array
     */
    public $typeMap = [
        'UInt8' => self::TYPE_SMALLINT,
        'UInt16' => self::TYPE_INTEGER,
        'UInt32' => self::TYPE_INTEGER,
        'UInt64' => self::TYPE_BIGINT,
        'Int8' => self::TYPE_SMALLINT,
        'Int16' => self::TYPE_INTEGER,
        'Int32' => self::TYPE_INTEGER,
        'Int64' => self::TYPE_BIGINT,
        'Float32' => self::TYPE_FLOAT,
        'Float64' => self::TYPE_FLOAT,
        'String' => self::TYPE_STRING,
        'FixedString' => self::TYPE_STRING,
        'DateTime' => self::TYPE_DATETIME,
        'Date' => self::TYPE_DATE,
        'Enum8' => self::TYPE_STRING,
        'Enum16' => self::TYPE_STRING
    ];

    /**
     * @inheritdoc
     */
    protected function loadTableSchema($name)
    {
        $table = new TableSchema;
        $this->resolveTableNames($table, $name);

        return $this->findColumns($table) ? $table : null;
    }

    /**
     * @inheritdoc
     */
    protected function findColumns($table)
    {
        $columns = $this->db->createCommand('SELECT * FROM system.columns WHERE table=:name', [':name' => $table->name])->queryAll();
        if ($columns === []) {
            return false;
        }
        foreach ($columns as $info) {
            $column = $this->loadColumnSchema($info);
            $table->columns[$column->name] = $column;
        }
        return true;
    }

    /**
     * @inheritdoc
     */
    protected function loadColumnSchema($info)
    {
        $column = new $this->columnSchemaClass;
        $column->name = $info['name'];
        $column->dbType = $info['type'];

        $column->unsigned = stripos($column->dbType, 'UInt') === 0 || stripos($column->dbType, '(UInt') !== false; // UInt64, Nullable(UInt32)

        foreach ($this->typeMap as $dbType => $type) {
            if (strncasecmp($column->dbType, $dbType, strlen($dbType)) === 0) {
                $column->type = $type;
                break 1;
            }
        }

        if (isset($info['default_type']) && $info['default_type'] !== '') {
            $column->defaultValue = $info['default_type'];
        }
        if (isset($info['default_kind']) && $info['default_kind'] !== '') {
            $column->defaultValue = $info['default_kind'];
        }

        if (
            $column->type === self::TYPE_STRING
            && preg_match('/^FixedString\((\d+)\)$/', $column->dbType, $out)
        ) {
            $column->size = (int)$out[1];
        }

        $column->phpType = $this->getColumnPhpType($column);
        return $column;
    }

    /**
     * @inheritdoc
     */
    public function quoteValue($str)
    {
        if (!is_string($str)) {
            return $str;
        }
        return "'" . addcslashes($str, "\000\n\r\\\032\047") . "'";
    }

    /**
     * @return QueryBuilder
     */
    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    /**
     * {@inheritdoc}
     */
    public function createColumnSchemaBuilder($type, $length = null)
    {
        return new ColumnSchemaBuilder($type, $length, $this->db);
    }
}