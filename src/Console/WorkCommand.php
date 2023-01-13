<?php

namespace LaravelQueue\Nsq\Console;

use Illuminate\Queue\Console\WorkCommand as BaseWorkCommand;
use Illuminate\Support\Facades\Config;

class WorkCommand extends BaseWorkCommand
{

    /**
     * Execute the console command.
     * @return int|null
     */
    public function handle()
    {
        $this->hasOption('queue') && Config::set(['consumer_topic' => $this->option('queue')]);

        if (method_exists(get_parent_class($this), 'handle')) {
            return parent::handle();
        }

        return parent::fire();

    }


}
