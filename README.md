## Laravel Nsq Client
NSQ client for laravel

## Requirements

| Dependency | Requirement                                                  |
| -------- |--------------------------------------------------------------|
| [PHP](https://secure.php.net/manual/en/install.php) | `>= 8.0`                                                     |
| [Swoole](https://www.swoole.co.uk/) | `The Newer The Better` `No longer support PHP5 since 2.0.12` |

## Installation
```
pecl install swoole
```
```
composer require lxy/nsq
```
## Usage
#### Set env
```
NSQSD_URL=127.0.0.1:4150
NSQLOOKUP_URL=127.0.0.1:4161

# If it is multiple, please separate them with ","
NSQSD_URL=127.0.0.1:4150,127.0.0.1:4151
```
#### Create Job
```
php artisan make:job NsqTestJob
```