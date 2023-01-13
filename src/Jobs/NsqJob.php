<?php

namespace LaravelQueue\Nsq\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use LaravelQueue\Nsq\Message\Packet;
use LaravelQueue\Nsq\Monitor\Consumer;
use LaravelQueue\Nsq\Queue\NsqQueue;


class NsqJob extends Job implements JobContract
{

    /**
     * The nsq queue instance.
     *
     * @var NsqQueue
     */
    protected NsqQueue $nsqQueue;

    /**
     * The nsq raw job payload.
     *
     * @var string
     */
    protected string $job;

    /**
     * The nsq top and channel name.
     *
     * @var string
     */
    protected $queue;

    /**
     * payload decode (job property)
     * @var array
     */
    protected array $decoded;


    /**
     * NsqJob constructor.
     * @param Container $container
     * @param NsqQueue $nsqQueue
     * @param $job
     * @param $connectionName
     * @param $queue
     */
    public function __construct(
        Container $container,
        NsqQueue $nsqQueue,
        $job,
        $connectionName,
        $queue
    )
    {
        $this->container = $container;
        $this->job = $job;
        $this->nsqQueue = $nsqQueue;
        $this->queue = $queue;
        $this->connectionName = $connectionName;
        $this->decoded = $this->payload();
    }

    /**
     * Get the number of times the job has been attempted.
     * @return int
     */
    public function attempts(): int
    {
        return Arr::get($this->decoded, 'attempts');
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody(): string
    {
        return $this->job;
    }

    /**
     * Delete the job from the queue.
     * success handle job
     * @return void
     */
    public function delete(): void
    {
        parent::delete();
        // sending to client set success
        $this->getCurrentClient()->send(Packet::fin($this->getJobId()));
        logger()->info("Process job success, send fin to nsq server.");
        // receive form client
        $this->getCurrentClient()->send(Packet::rdy(Config::get('nsq.options.rdy', 1)));
        logger()->info("Ready to receive next message.");
    }


    /**
     * Re-queue a message
     * @param int $delay
     * @return void
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        // re push job to nsq queue
        $this->getCurrentClient()->send(Packet::req($this->getJobId(), $delay));
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId(): string
    {
        return Arr::get($this->decoded, 'id');
    }

    /**
     * get nsq swoole client pool
     * @return NsqQueue
     */
    public function getNsqQueue(): NsqQueue
    {
        return $this->nsqQueue;
    }

    /**
     * get current nsq swoole client
     * @return Consumer
     */
    public function getCurrentClient(): Consumer
    {
        return $this->getNsqQueue()->getCurrentClient();
    }

    /**
     * get nsq body message
     * @return array|\ArrayAccess|mixed
     */
    public function getMessage(): mixed
    {
        return Arr::get($this->decoded, 'message');
    }
}
