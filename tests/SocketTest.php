<?php
namespace bashkarev\clickhouse\tests;

use bashkarev\clickhouse\Configuration;
use bashkarev\clickhouse\Socket;
use bashkarev\clickhouse\SocketException;

/**
 * @author Sartor <sartorua@gmail.com>
 */
class SocketTest extends TestCase
{
    private $server;
    private $port;

    protected function setUp()
    {
        parent::setUp();

        $this->prepareDummyServer();
    }

    protected function tearDown()
    {
        $this->shutdownDummyServer();

        parent::tearDown();
    }

    private function prepareDummyServer()
    {
        $this->server = stream_socket_server("tcp://0.0.0.0:0");
        $name = stream_socket_get_name($this->server, 0);

        $this->port = substr($name, strpos($name, ':') + 1);
    }

    private function shutdownDummyServer()
    {
        fclose($this->server);
    }

    private function socket(): Socket
    {
        $config = new Configuration('host=0.0.0.0;port='.$this->port, '', '');

        return new Socket($config);
    }

    public function testOpen()
    {
        $socket = $this->socket();

        $socket->close();
    }

    public function testWrite()
    {
        $socket = $this->socket();

        $client = stream_socket_accept($this->server);

        $socket->write("Test string");

        $socket->close();

        $result = fgets($client);

        $this->assertEquals("Test string", $result);
    }

    public function testBrokenPipe()
    {
        $this->expectException(SocketException::class);
        $socket = $this->socket();

        $socket->write("Write to valid pipe");

        $socket->close();

        $socket->write("Write to broken pipe");
    }

    public function testRead()
    {
        $socket = $this->socket();

        $client = stream_socket_accept($this->server);

        fwrite($client, "Test read string");
        fclose($client);

        $result = fgets($socket->getNative());

        $this->assertEquals("Test read string", $result);
    }
}