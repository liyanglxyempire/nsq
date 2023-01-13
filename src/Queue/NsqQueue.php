<?php

namespace LaravelQueue\Nsq\Queue;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Support\Facades\Config;
use LaravelQueue\Nsq\Exception\SubscribeException;
use LaravelQueue\Nsq\Adapter\NsqClientManager;
use LaravelQueue\Nsq\Exception\FrameException;
use LaravelQueue\Nsq\Exception\PublishException;
use LaravelQueue\Nsq\Jobs\NsqJob;
use LaravelQueue\Nsq\Message\Packet;
use LaravelQueue\Nsq\Message\Unpack;
use LaravelQueue\Nsq\Monitor\Consumer;

class NsqQueue extends Queue implements QueueContract
{
    const PUB_ONE = 1;
    const PUB_TWO = 2;
    const PUB_QUORUM = 5;

    /**
     * The name of the default topic.
     *
     * @var string
     */
    protected string $default;

    /**
     * The name of the topic.
     * @var string
     */
    protected string $topicName;

    /**
     * nsq tcp client pool
     * @var NsqClientManager
     */
    protected NsqClientManager $pool;


    /**
     * nsq pub number
     * @var integer
     */
    protected int $pubSuccessCount;

    /**
     * The expiration time of a job.
     *
     * @var int
     */
    protected int $retryAfter = 60;

    /**
     * current nsq tcp client
     */
    protected Consumer  $currentClient;

    /**
     * NsqQueue constructor.
     * @param NsqClientManager $client
     * @param string $default default topic name
     * @param int $retryAfter
     */
    public function __construct(
        NsqClientManager $client,
        string $default,
        int $retryAfter = 60,
    )
    {
        $this->pool = $client;
        $this->default = $default;
        $this->retryAfter = $retryAfter;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    /**
     * @param  string  $job
     * @param  mixed  $data
     * @param  string|null  $queue  nsq topic name
     * @return mixed
     */
    public function push($job, mixed $data = '', $queue = null): mixed
    {
        $this->topicName = $this->getTopic($queue);
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getTopic($this->topicName), $data),
            $queue,
            null,
            function ($payload, $queue) {
                return $this->pushRaw($payload, $queue);
            }
        );
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string $queue
     * @param  array $options
     * @return NsqQueue
     */
    public function pushRaw($payload, $queue = null, array $options = []): NsqQueue
    {
        $payload = json_decode($payload, true);
        if (empty($payload['data'])) {
            $payload['data'] = unserialize($payload['job'])->payload;
        }

        return $this->publishTo(Config::get('nsq.options.cl', 1))->publish($this->topicName, json_encode($payload));
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $this->topicName = $this->getTopic($queue);
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getTopic($this->topicName), $data),
            $queue,
            $delay,
            function ($payload, $queue) {
                return $this->pushRaw($payload, $queue);
            }
        );
    }

    public function pop($queue = null): ?NsqJob
    {
        $queue = $this->getTopic($queue);

        try {
            $response = null;
            foreach ($this->pool->getConsumerPool() as $key => $client) {
                // if lost connection  try connect
                $this->currentClient = $client;

                $data = $this->currentClient->receive();
                // if no message return null
                if (!$data) continue;
                // unpack message
                $frame = Unpack::getFrame($data);

                if (Unpack::isHeartbeat($frame)) {
                    $this->currentClient->send(Packet::nop());
                } elseif (Unpack::isOk($frame)) {
                    continue;
                } elseif (Unpack::isError($frame)) {
                    continue;
                } elseif (Unpack::isMessage($frame)) {
                    $rawBody = $this->adapterNsqPayload($frame);
                    logger()->info("Ready to process job.");
                    $response = new NsqJob($this->container, $this, $rawBody, $this->connectionName, $queue ?: $this->default);
                }
            }

            $this->refreshClient();

            return $response;

        } catch (\Throwable $exception) {
            throw new SubscribeException($exception->getMessage());
        }
    }

    /**
     * Get the topic or return the default.
     *
     * @param string|null $queue
     * @return string
     */
    public function getTopic(?string $queue): string
    {
        return $queue ?: $this->default;
    }


    /**
     * Define nsqd hosts to publish to
     *
     * We'll remember these hosts for any subsequent publish() call, so you
     * only need to call this once to publish
     *
     * @param int $cl      Consistency level - basically how many `nsqd`
     *                     nodes we need to respond to consider a publish successful
     *                     The default value is nsqphp::PUB_ONE
     *
     * @return $this
     * @throws \InvalidArgumentException If we cannot achieve the desired CL
     *      (eg: if you ask for PUB_TWO but only supply one node)
     *
     * @throws \InvalidArgumentException If bad CL provided
     */
    public function publishTo(int $cl = self::PUB_ONE): static
    {

        $producerPoolSize = count($this->pool->getProducerPool());

        $this->pubSuccessCount = match ($cl) {
            self::PUB_ONE, self::PUB_TWO => $cl,
            self::PUB_QUORUM => ceil($producerPoolSize / 2) + 1,
            default => throw new FrameException('Invalid consistency level'),
        };

        if ($this->pubSuccessCount > $producerPoolSize) {
            throw new PublishException(
                sprintf('Cannot achieve desired consistency level with %s nodes', $producerPoolSize)
            );
        }

        return $this;
    }

    /**
     * Publish message
     *
     * @param string $topic     A valid topic name: [.a-zA-Z0-9_-] and 1 < length < 32
     * @param array|string $msg array: multiple messages
     * @param int $tries        Retry times
     *
     * @return $this
     * @throws PublishException If we don't get "OK" back from server
     *      (for the specified number of hosts - as directed by `publishTo`)
     *
     */
    public function publish(string $topic, array|string $msg, int $tries = 1): static
    {
        $producerPool = $this->pool->getProducerPool();
        // pick a random
        shuffle($producerPool);

        $success = 0;
        $errors = [];
        // TODO 这一块需要重构，逻辑不太合理，可能陷入死循环 liyang 2023/1/10 14:27
        foreach ($producerPool as $producer) {
            try {
                for ($run = 1; $run <= $tries; $run++) {
                    try {
                        $payload = is_array($msg) ? Packet::mpub($topic, $msg) : Packet::pub($topic, $msg);
                        $producer->send($payload);
                        $frame = Unpack::getFrame($producer->receive());

                        while (Unpack::isHeartbeat($frame)) {
                            $producer->send(Packet::nop());
                            $frame = Unpack::getFrame($producer->receive());
                        }

                        if (Unpack::isOK($frame)) {
                            $success++;
                        } else {
                            $errors[] = $frame['error'];
                        }

                        break;
                    } catch (\Throwable $e) {
                        if ($run >= $tries) {
                            throw $e;
                        }

                        $producer->reconnect();
                    }
                }
            } catch (\Throwable $e) {
                $errors[] = $e->getMessage();
            }

            if ($success >= $this->pubSuccessCount) {
                break;
            }
        }

        if ($success < $this->pubSuccessCount) {
            throw new PublishException(
                sprintf('Failed to publish message; required %s for success, achieved %s. Errors were: %s',
                    $this->pubSuccessCount,
                    $success,
                    implode(', ', $errors)
                )
            );
        }

        return $this;
    }


    /**
     * Get the connection for the queue.
     * @return Consumer
     */
    public function getCurrentClient(): Consumer
    {
        return $this->currentClient;
    }

    /**
     * adapter nsq queue job body type
     * @param array $data
     * @return string
     * @throws \Exception
     */
    protected function adapterNsqPayload(array $data): string
    {
        $message = json_decode($data['message'], true);

        $payload = json_encode(array_merge(
            $message,
            [
                'attempts' => $data['attempts'],
                'id'       => $data['id'],
                'message'     => $message,
            ]
        ));

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new \Exception(
                'Unable to JSON encode payload. Error code: ' . json_last_error()
            );
        }

        return $payload;
    }


    /**
     * refresh nsq client form nsqlookupd result
     * @return void
     * @throws \ReflectionException
     */
    protected function refreshClient(): void
    {
        // check connect time
        $connectTime = $this->pool->getConnectTime();
        if (time() - $connectTime >= 60 * 5) {
            foreach ($this->pool->getConsumerPool() as $key => $client) {
                $client->close();
            }
            $queueManager = app('queue');
            $reflect = new \ReflectionObject($queueManager);
            $property = $reflect->getProperty('connections');
            $property->setAccessible(true);
            //remove nsq
            $connections = $property->getValue($queueManager);
            unset($connections['nsq']);
            $property->setValue($queueManager, $connections);
            logger()->info("refresh nsq client success.");
        }
    }
}
