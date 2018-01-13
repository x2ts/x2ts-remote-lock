<?php
/**
 * Created by IntelliJ IDEA.
 * User: rek
 * Date: 2018/1/12
 * Time: 下午4:55
 */

namespace x2ts\remote_lock;


use x2ts\Component;
use x2ts\SocketException;
use x2ts\ComponentFactory as X;
use x2ts\SocketTimeoutException;

/**
 * Class RWLock
 *
 * @package x2ts\remote_lock
 * @property-read resource $connection
 */
class RWLock extends Component {
    public static $_conf = [
        'host'    => 'localhost',
        'port'    => 2211,
        'timeout' => 10,
    ];

    protected $_connection;

    /**
     * @throws SocketException
     */
    public function getConnection() {
        if (!is_resource($this->_connection)) {
            $this->_connection = @fsockopen(
                $this->conf['host'],
                $this->conf['port'],
                $errno,
                $error,
                $this->conf['timeout']
            );
            if ($this->_connection === false) {
                throw new SocketException($error, $errno);
            }
        }
        return $this->_connection;
    }

    public function close() {
        if (is_resource($this->_connection)) {
            fclose($this->_connection);
            $this->_connection = null;
        } else {
            $this->_connection = null;
        }
    }

    /**
     * @param string $action
     * @param string $key
     *
     * @throws SocketException
     */
    public function send(string $action, string $key) {
        $connection = $this->getConnection();
        fwrite($connection, "$action $key\r\n");
        $i = stream_get_meta_data($this->connection);
        if ($i['timed_out']) {
            X::logger()->info($i);
            throw new SocketTimeoutException('Send request timeout', 1);
        }
        fflush($connection);
    }

    /**
     * @return bool|string
     * @throws SocketTimeoutException
     * @throws SocketException
     */
    public function recv() {
        $r = fgets($this->connection);
        $i = stream_get_meta_data($this->connection);
        if ($i['timed_out']) {
            X::logger()->info($i);
            throw new SocketTimeoutException('Fetch response timeout', 2);
        } elseif ($r === false && $i['eof']) {
            $this->close();
            throw new SocketException('Connection lost');
        }

        return trim($r);
    }

    /**
     * @param string $key
     * @param int    $timeout
     *
     * @throws LockException
     * @throws SocketException
     * @throws SocketTimeoutException
     */
    public function share(string $key, int $timeout = 30) {
        X::logger()->trace("Taking share lock $key");
        stream_set_timeout($this->connection, $timeout);
        $this->send('SH', md5($key));
        $r = $this->recv();
        if ($r === 'OK') {
            X::logger()->trace('OK');
            return;
        } elseif ($r === 'DUP') {
            X::logger()->warn("Taking taken share lock $key");
            return;
        } elseif (0 === strpos($r, 'ERR')) {
            throw new LockException(substr($r, 4), 1);
        } else {
            X::logger()->warn('server response: ' . $r);
            throw new LockException('Unknown response');
        }
    }

    /**
     * @param string $key
     * @param int    $timeout
     *
     * @throws LockException
     * @throws SocketException
     * @throws SocketTimeoutException
     */
    public function lock(string $key, int $timeout = 30) {
        X::logger()->trace("Taking exclusive lock $key");
        stream_set_timeout($this->connection, $timeout);
        $this->send('EX', md5($key));
        $r = $this->recv();
        if ($r === 'OK') {
            X::logger()->trace('OK');
            return;
        } elseif ($r === 'DUP') {
            X::logger()->warn("Taking taken exclusive lock $key");
            return;
        } elseif (0 === strpos($r, 'ERR')) {
            throw new LockException(substr($r, 4), 1);
        } else {
            X::logger()->warn('server response: ' . $r);
            throw new LockException('Unknown response');
        }
    }

    /**
     * @param string $key
     * @param int    $timeout
     *
     * @throws LockException
     * @throws SocketException
     * @throws SocketTimeoutException
     */
    public function release(string $key, int $timeout = 30) {
        X::logger()->trace("Releasing lock $key");
        stream_set_timeout($this->connection, $timeout);
        $this->send('UN', md5($key));
        $r = $this->recv();
        if ($r === 'OK') {
            X::logger()->trace('OK');
            return;
        } elseif (0 === strpos($r, 'ERR')) {
            throw new LockException(substr($r, 4), 1);
        } else {
            X::logger()->warn('server response: ' . $r);
            throw new LockException('Unknown response');
        }
    }

    public function __destruct() {
        $this->close();
    }
}