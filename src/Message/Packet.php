<?php

namespace LaravelQueue\Nsq\Message;

class Packet
{
    const MAGIC = "  V2";
    const IDENTIFY = "IDENTIFY";
    const PING = "PING";
    const SUB = "SUB";
    const PUB = "PUB";
    const MPUB = "MPUB";
    const RDY = "RDY";
    const FIN = "FIN";
    const REQ = "REQ";
    const TOUCH = "TOUCH";
    const CLS = "CLS";
    const NOP = "NOP";
    const AUTH = "AUTH";


    /**
     * "Magic" identifier - for version we support
     *
     * @return string
     */
    public static function magic(): string
    {
        return self::MAGIC;
    }

    /**
     * Update client metadata on the server and negotiate features
     *
     * @param array $config
     * @return string
     */
    public static function identify(array $config): string
    {
        return self::packet(self::IDENTIFY, null, json_encode($config));
    }

    /**
     * Liveness
     *
     * @return string
     */
    public static function ping(): string
    {
        return self::packet(self::PING);
    }

    /**
     * Subscribe to a topic/channel
     *
     * @param string $topic
     * @param string $channel
     * @return string
     */
    public static function sub(string $topic, string $channel): string
    {
        return self::packet(self::SUB, [$topic, $channel]);
    }

    /**
     * Publish a message to a topic
     *
     * @param string $topic
     * @param string $data
     * @return string
     */
    public static function pub(string $topic, string $data): string
    {
        return self::packet(self::PUB, $topic, $data);
    }

    /**
     * Publish multiple messages to a topic - atomically
     *
     * @param string $topic
     * @param array $data
     * @return string
     */
    public static function mpub(string $topic, array $data): string
    {
        $msgs = '';
        foreach ($data as $value) {
            $msgs .= pack("N", strlen($value)) . $value;
        }

        return sprintf("%s %s\n%s%s%s", self::MPUB, $topic, pack("N", strlen($msgs)), pack("N", count($data)), $msgs);
    }

    /**
     * Update RDY state - indicate you are ready to receive N messages
     *
     * @param int $count
     * @return string
     */
    public static function rdy(int $count): string
    {
        return self::packet(self::RDY, $count);
    }


    public static function timeout($time): string
    {
        return self::packet('max-msg-timeout', $time);
    }

    /**
     * Finish a message
     *
     * @param string $message_id
     * @return string
     */
    public static function fin(string $message_id): string
    {
        return self::packet(self::FIN, $message_id);
    }

    /**
     * Re-queue a message
     *
     * @param string $message_id
     * @param int $timeout In microseconds
     * @return string
     */
    public static function req(string $message_id, int $timeout): string
    {
        return self::packet(self::REQ, [$message_id, $timeout]);
    }

    /**
     * Reset the timeout for an in-flight message
     *
     * @param string $message_id
     * @return string
     */
    public static function touch(string $message_id): string
    {
        return self::packet(self::TOUCH, $message_id);
    }

    /**
     * Cleanly close
     *
     * @return string
     */
    public static function cls(): string
    {
        return self::packet(self::CLS);
    }

    /**
     * No-op
     *
     * @return string
     */
    public static function nop(): string
    {
        return self::packet(self::NOP);
    }

    /**
     * Auth for server
     *
     * @param string $password
     * @return string
     */
    public static function auth(string $password): string
    {
        return self::packet(self::AUTH, null, $password);
    }

    /**
     * Pack string
     *
     * @param string $cmd
     * @param mixed|null $params
     * @param mixed|null $data
     * @return string
     */
    private static function packet(string $cmd, mixed $params = null, mixed $data = null): string
    {
        if (is_array($params)) {
            $params = implode(' ', $params);
        }

        if ($data !== null) {
            $data = pack('N', strlen($data)) . $data;
        }

        return sprintf("%s %s\n%s", $cmd, $params, $data);
    }
}
