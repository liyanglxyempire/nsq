<?php

namespace LaravelQueue\Nsq;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use LaravelQueue\Nsq\Provider\WorkCommandProvider;
use LaravelQueue\Nsq\Queue\Connectors\NsqConnector;

class ServiceProvider extends BaseServiceProvider
{
    /**
     * Register services.
     *
     * @return void
     */
    public function register(): void
    {
        $source = realpath($raw = __DIR__.'/../config/nsq.php') ?: $raw;
        $this->mergeConfigFrom($source, 'queue.connections.nsq');
    }

    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot(): void
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        $queue->addConnector('nsq', function () {
            return new NsqConnector;
        });

        // add defer provider, rebind work command
        $this->app->addDeferredServices([WorkCommandProvider::class]);
    }
}
