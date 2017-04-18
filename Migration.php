<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\di\Instance;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Migration extends \yii\db\Migration
{

    public $db = 'clickhouse';

    /**
     * @inheritdoc
     */
    public function init()
    {
        $this->db = Instance::ensure($this->db, Connection::className());
        $this->db->getSchema()->refresh();
    }

    /**
     * @inheritdoc
     */
    public function up()
    {

    }

    /**
     * @inheritdoc
     */
    public function down()
    {

    }

}