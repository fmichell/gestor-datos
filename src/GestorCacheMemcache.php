<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 01-19-17/09:06 PM
 *
 * Description: 
 */

namespace Vigoron\GestorDatos;
/**
 * Class GestorCacheMemcache
 * @package Vigoron\GestorDatos
 */
class GestorCacheMemcache {

    /**
     * @var \Memcache
     */
    public $mc;

    /**
     * Inicia conexion con el servidor de cache.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->mc = new \Memcache();

        if (!empty($config['servidores'])) {
            foreach ($config['servidores'] as $llave => $servidor) {
                $conexion = @fsockopen($servidor[0], $servidor[1], $errno, $errstr, 0.02);
                if (!$conexion) {
                    unset($config['servidores'][$llave]);
                    continue;
                } else {
                    fclose($conexion);
                }
                $this->mc->addserver($servidor[0], $servidor[1]);
            }
        }
    }

    /**
     * Retorna conexion con servidor de cache.
     *
     * @return \Memcache
     */
    public function obtenerGestor()
    {
        return $this->mc;
    }

} 