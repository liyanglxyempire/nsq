<?php

namespace LaravelQueue\Nsq\Message;


use LaravelQueue\Nsq\Exception\FrameException;

class Unpack
{
    /**
     * Frame types
     */
    const FRAME_TYPE_RESPONSE = 0;     //no response
    const FRAME_TYPE_ERROR = 1;     //error response
    const FRAME_TYPE_MESSAGE = 2;     //message response

    /**
     * Heartbeat response content
     */
    const HEARTBEAT = '_heartbeat_';

    /**
     * OK response content
     */
    const OK = 'OK';


    /**
     * Read frame
     *
     * @param string $buffer
     * @return array With keys: type, data
     *@throws FrameException
     */
    public static function getFrame(string $buffer): array
    {
        $frame = [
            'size' => self::getInt(substr($buffer, 0, 4)),
            'type' => self::getInt(substr($buffer, 4, 4))
        ];


        switch ($frame['type']) {
            case self::FRAME_TYPE_RESPONSE:
                $frame['message'] = substr($buffer, 8);
                break;

            case self::FRAME_TYPE_ERROR:
                $frame['error'] = substr($buffer, 8);
                break;

            case self::FRAME_TYPE_MESSAGE:
                //nanosecond unix timestamp
                $frame['timestamp'] = self::getLong(substr($buffer, 8, 8));
                // queue retry attempts
                $frame['attempts'] = self::getShort(substr($buffer, 16, 2));
                //message id
                $frame['id'] = self::getString(substr($buffer, 18, 16));
                //message body
                $frame['message'] = substr($buffer, 34);
                break;

            default:
                throw new FrameException(substr($buffer, 8));
        }

        return $frame;
    }

    /**
     * Test if frame is a message frame
     *
     * @param array $frame
     * @param $type
     * @param $response
     *
     * @return bool
     */
    private static function checkMessage(array $frame, $type, $response = null): bool
    {
        return isset($frame['type'], $frame['message'])
            && $frame['type'] === $type
            && ($response === NULL || $frame['message'] === $response);
    }

    /**
     * Test if is an responsee
     *
     * @param $frame
     *
     * @return bool
     */
    public static function isResponse($frame): bool
    {
        return self::checkMessage($frame, self::FRAME_TYPE_RESPONSE);
    }

    /**
     * Test if frame is a message frame
     *
     * @param $frame
     *
     * @return bool
     */
    public static function isMessage($frame): bool
    {
        return self::checkMessage($frame, self::FRAME_TYPE_MESSAGE);
    }

    /**
     * Test if frame is heartbeat
     *
     * @param $frame
     *
     * @return bool
     */
    public static function isHeartbeat($frame): bool
    {
        //确切的说心跳检查不属于响应类型。也没有第4-8字节表示消息类型，但是此处为了处理方便将其归结为响应类型
        return self::checkMessage($frame, self::FRAME_TYPE_RESPONSE, self::HEARTBEAT);
    }

    /**
     * Test if frame is OK
     *
     * @param $frame
     *
     * @return bool
     */
    public static function isOk($frame): bool
    {
        return self::checkMessage($frame, self::FRAME_TYPE_RESPONSE, self::OK);
    }

    /**
     * Test if frame is an error esponse
     *
     * @param $frame
     *
     * @return bool
     */
    public static function isError($frame): bool
    {
        return self::checkMessage($frame, self::FRAME_TYPE_ERROR);
    }

    /**
     * Read and unpack integer (4 bytes)
     *
     * @param $param
     *
     * @return int
     */
    private static function getInt($param): int
    {
        list(, $size) = unpack('N', $param);
        if ((PHP_INT_SIZE !== 4)) {
            $size = sprintf("%u", $size);
        }

        return (int)$size;
    }

    /**
     * @param $param
     *
     * @return mixed
     */
    private static function getShort($param): mixed
    {
        list(, $res) = unpack('n', $param);

        return $res;
    }

    /**
     * Read and unpack long (8 bytes)
     *
     * @param $param
     *
     * @return string
     */
    private static function getLong($param): string
    {
        $hi = unpack('N', substr($param, 0, 4));
        $lo = unpack('N', substr($param, 4, 4));

        // workaround signed/unsigned braindamage in php
        $hi = sprintf("%u", $hi[1]);
        $lo = sprintf("%u", $lo[1]);

        return bcadd(bcmul($hi, "4294967296"), $lo);
    }

    /**
     * Read and unpack string
     * @param $param
     * @param int $size
     * @return string
     */
    private static function getString($param, int $size = 16): string
    {
        $temp = unpack("c{$size}chars", $param);
        $out = "";
        foreach ($temp as $v) {
            if ($v > 0) {
                $out .= chr($v);
            }
        }

        return $out;
    }
}
