<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 01-19-17/09:07 PM
 *
 * Description: 
 */

namespace fmichell\GestorDatos;
/**
 * Class GestorCacheMemcached
 * @package fmichell\GestorDatos
 */
class GestorCacheMemcached {

    /**
     * @var \Memcached
     */
    public $mc;

    /**
     * Inicia conexion con el servidor de cache.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->mc = new \Memcached();
        $this->mc->setOption(\Memcached::OPT_NO_BLOCK, true);
        $this->mc->setOption(\Memcached::OPT_TCP_NODELAY, true);
        $this->mc->setOption(\Memcached::OPT_CONNECT_TIMEOUT, 10); // 50
//        $this->mc->setOption(\Memcached::OPT_RETRY_TIMEOUT, 1);
        $this->mc->setOption(\Memcached::OPT_POLL_TIMEOUT, 20);
        $this->mc->setOption(\Memcached::OPT_REMOVE_FAILED_SERVERS, true);

        if (!empty($config['servidores'])) {
            foreach ($config['servidores'] as $llave => $servidor) {
                $conexion = @fsockopen($servidor[0], $servidor[1], $errno, $errstr, 0.02);
                if (!$conexion) {
                    unset($config['servidores'][$llave]);
                } else {
                    fclose($conexion);
                }
            }
            $this->mc->addServers($config['servidores']);
        }
    }

    /**
     * Retorna conexion con servidor de cache.
     * 
     * @return \Memcached
     */
    public function obtenerGestor()
    {
        return $this->mc;
    }

} 