<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 06-15-16/09:24 AM
 * Version: 1.0
 *
 * Description: 
 */

namespace fmichell\GestorDatos;
/**
 * Class GestorMySQL
 * @package fmichell\GestorDatos
 */
class GestorMySQL extends GestorDB {

    const DRIVER = 'mysql:';

    /**
     * Inicia una conexion con la base de datos.
     *
     * @param array|null $configuracion
     * @return $this
     * @throws \ErrorException
     */
    public function conectar($configuracion = null)
    {
        // Configuramos de ser necesario
        if ($configuracion)
            $this->configurar($configuracion);

        // Preparamos cadena de conexion
        $cadenaConexion = array(
            'host'      => ($this->_config['servidor']) ? $this->_config['servidor'] : null,
            'port'      => ($this->_config['puerto'] ? $this->_config['puerto'] : null),
            'dbname'    => ($this->_config['basedatos'] ? $this->_config['basedatos'] : null),
            'charset'   => ($this->_config['charset'] ? $this->_config['charset'] : null),
        );
        foreach($cadenaConexion as $k => $c) {
            if (empty($c))
                unset($cadenaConexion[$k]);
            else
                $cadenaConexion[$k] = $k . '=' . $c;
        }
        $cadenaConexion = self::DRIVER . implode(';', $cadenaConexion);

        // Realizamos conexion con la BD
        try {

            $this->_conexion = new \PDO($cadenaConexion,
                $this->_config['usuario'],
                $this->_config['contrasena'],
                array(
                    \PDO::ATTR_PERSISTENT => ($this->_config['persistente']) ? true : false,
                )
            );

            // Forzamos que los nombres de las columnas vengan en minuscula
            $this->_conexion->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
            // Las cadenas vacias son convertidas en NULL
            $this->_conexion->setAttribute(\PDO::ATTR_ORACLE_NULLS, \PDO::NULL_EMPTY_STRING);
            // Las cadenas vacias son convertidas en NULL
            $this->_conexion->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            // Estableciendo timeout para la conexion
            $this->_conexion->setAttribute(\PDO::ATTR_TIMEOUT, 10);

        } catch (\PDOException $e) {

            throw new \ErrorException(get_class($this) . ' | Error al conectar con la BD. La BD dio el siguiente error: ' . utf8_encode($e->getMessage()),
                (int)$e->getCode()
            );

        }

        return $this;

    }

}