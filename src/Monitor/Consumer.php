<?php

namespace LaravelQueue\Nsq\Monitor;


use Illuminate\Support\Arr;
use LaravelQueue\Nsq\Message\Packet;
use Swoole\Client;

class Consumer extends AbstractMonitor
{

    /**
     * Subscribe topic
     *
     * @var string
     */
    protected string $topic;

    /**
     * Subscribe channel
     *
     * @var string
     */
    protected string $channel;

    /**
     * Nsqd config
     *
     * @var array
     */
    protected array $config;

    /**
     * Nsqd host
     *
     * @var string
     */
    protected string $host;


    /**
     * Consumer constructor.
     * @param string $host
     * @param array $config
     * @param string $topic
     * @param string $channel
     * @throws \Exception
     */
    public function __construct(string $host, array $config, string $topic, string $channel)
    {
        $this->host = $host;
        $this->config = $config;
        $this->topic = $topic;
        $this->channel = $channel;
        $this->connect();

    }

    /**
     * @throws \Exception
     */
    public function connect()
    {
        // init swoole client
        $this->client = new Client(SWOOLE_SOCK_TCP | SWOOLE_KEEP, SWOOLE_SOCK_SYNC);

        // set swoole tcp client config
        $this->client->set(Arr::get($this->config, 'client.options'));

        list($host, $port) = explode(':', $this->host);
        // connect nsq server
        if (!$this->client->connect($host, $port, 3)) {
            throw new \Exception('connect nsq server failed.');
        }
        // send magic to nsq server
        $this->client->send(Packet::magic());

        // send identify params
        $this->client->send(Packet::identify(Arr::get($this->config, 'identify')));

        // sub nsq topic and channel
        $this->client->send(Packet::sub($this->topic, $this->channel));

        // tell nsq server to be ready accept {n} data
        $this->client->send(Packet::rdy(Arr::get($this->config, 'options.rdy', 1)));
    }
}
