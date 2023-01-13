<?php

namespace LaravelQueue\Nsq\Provider;


use Illuminate\Queue\Console\WorkCommand as QueueWorkCommand;
use Illuminate\Support\ServiceProvider;
use LaravelQueue\Nsq\Console\WorkCommand;

class WorkCommandProvider extends ServiceProvider
{

    protected bool $defer = true;

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register(): void
    {
        // rebind queue console command
        $this->app->singleton(QueueWorkCommand::class, function ($app) {
            return new WorkCommand($app['queue.worker'],  $app['cache.store']);
        });
    }

    /**
     * @return array
     */
    public function provides(): array
    {
        return ['command.queue.work'];
    }

}
