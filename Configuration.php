<?php
/**
 * @copyright Copyright (c) 2017 Dmitry Bashkarev
 * @license https://github.com/bashkarev/clickhouse/blob/master/LICENSE
 * @link https://github.com/bashkarev/clickhouse#readme
 */

namespace bashkarev\clickhouse;

use yii\base\InvalidParamException;

/**
 * @author Dmitry Bashkarev <dmitry@bashkarev.com>
 */
class Configuration
{
    /**
     * @var string
     */
    protected $address;
    /**
     * @var string
     */
    protected $url = '/';
    /**
     * @var array
     */
    protected $options = [];

    /**
     * Configuration constructor.
     * @param string $dsn
     * @param string|null $user
     * @param string|null $password
     */
    public function __construct($dsn, $user, $password)
    {
        $this->prepare($dsn);
        if ($user !== null && $user !== '') {
            $this->options['user'] = $user;
        }
        if ($password !== null && $password !== '') {
            $this->options['password'] = $password;
        }
    }

    /**
     * @param array $options
     * @return string
     */
    public function prepareUrl($options = [])
    {
        if ($options === [] && $this->options === []) {
            return $this->url;
        }
        if (isset($options['user']) || isset($options['password'])) {
            throw new InvalidParamException('Do not change user or password');
        }
        return $this->url . '?' . http_build_query(array_merge($this->options, $options));
    }

    /**
     * @return string Address to the socket to connect to.
     */
    public function getAddress()
    {
        return $this->address;
    }

    /**
     * @param string $dsn
     */
    protected function prepare($dsn)
    {
        foreach (explode(';', $dsn) as $item) {
            if ($item === '' || strpos($item, '=') === false) {
                continue;
            }
            list($key, $value) = explode('=', $item);
            $this->options[$key] = $value;
        }

        $this->address = 'tcp://';
        if (isset($this->options['host'])) {
            $this->address .= $this->options['host'];
            unset($this->options['host']);
        } else {
            $this->address .= '127.0.0.1';
        }
        if (isset($this->options['port'])) {
            $this->address .= ":{$this->options['port']}";
            unset($this->options['port']);
        } else {
            $this->address .= ':8123';
        }
    }

}