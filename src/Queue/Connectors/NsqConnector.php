<?php

namespace LaravelQueue\Nsq\Queue\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use LaravelQueue\Nsq\Adapter\NsqClientManager;
use LaravelQueue\Nsq\Queue\NsqQueue;

class NsqConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     * @param array $config
     * @return NsqQueue
     * @throws \Exception
     */
    public function connect(array $config): NsqQueue
    {
        $client = new NsqClientManager($config);

        return new NsqQueue(
            $client,
            $config['queue'],
            Arr::get($config, 'retry_delay_time', 60),
        );
    }
}
