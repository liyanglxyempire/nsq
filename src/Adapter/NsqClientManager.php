<?php

namespace LaravelQueue\Nsq\Adapter;

use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use LaravelQueue\Nsq\Lookup\Lookup;
use LaravelQueue\Nsq\Monitor\Consumer;
use LaravelQueue\Nsq\Monitor\Producer;

class NsqClientManager
{
    /**
     * @var array
     */
    protected array $config;

    /**
     * nsq tcp pub client pool
     * @var array
     */
    protected array $producerPool = [];

    /**
     * nsq tcp sub client pool
     * @var
     */
    protected array $consumerPool = [];

    /**
     * connect time
     * @var integer
     */
    protected int $connectTime;

    protected array $topics = [];

    /**
     * @param array $config
     * @throws \Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->connect();
    }

    /**
     * @return void
     * @throws \Exception
     */
    protected function connect(): void
    {
        $this->connectTime = time();

        // TODO 需要根据使用者身份生产者还是消费者获取不同的连接 liyang 2023/1/10 10:15
        // TODO topic配置方案，如何获取topic，如何获取所有的topic liyang 2023/1/10 16:24
        if (app()->runningInConsole()) {
            $this->getTopics();

            $lookup = new Lookup(Arr::get($this->config, 'connection.nsqlookup_url', ['127.0.0.1:4161']));

            foreach ($this->topics as $topic) {
                $nsqdList = $lookup->lookupHosts($topic);
                foreach ($nsqdList['lookupHosts'] as $item) {
                    $consumer = new Consumer($item, $this->config, $topic, $this->config['channel']);

                    $this->consumerPool[$item] = $consumer;
                }
            }
        }

        $hosts = Arr::get($this->config, 'connection.nsqd_url', ['127.0.0.1:4150']);
        foreach ($hosts as $item) {
            $producer = new Producer($item, $this->config);
            $this->producerPool[$item] = $producer;
        }
    }


    /**
     * get nsq pub client pool
     * @return array
     */
    public function getProducerPool(): array
    {
        return $this->producerPool;
    }

    /**
     * get nsq sub client pool
     * @return array
     */
    public function getConsumerPool(): array
    {
        return $this->consumerPool;
    }

    public function getTopics(): void
    {
        $this->topics = !empty(Config::get('consumer_topic')) ? explode(',', Config::get('consumer_topic')) : ['default'];
    }

    /**
     * get connect time
     * @return int
     */
    public function getConnectTime(): int
    {
        return $this->connectTime;
    }
}
