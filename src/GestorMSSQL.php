<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 06-15-18/11:16 AM
 * Version: 1.0
 *
 * Description: 
 */

namespace fmichell\GestorDatos;
/**
 * Class GestorMSSQL
 * @package fmichell\GestorDatos
 */
class GestorMSSQL extends GestorDB {

    const DRIVER = 'dblib:';

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
        $cadenaConexion = self::DRIVER . 'host=' . $this->_config['servidor'] . ':' . $this->_config['puerto'] . ';';
        $cadenaConexion.= 'dbname=' . $this->_config['basedatos'];

        // Realizamos conexion con la BD
        try {

            $this->_conexion = new \PDO($cadenaConexion,
                $this->_config['usuario'],
                $this->_config['contrasena']
            );

            // Forzamos que los nombres de las columnas vengan en minuscula
            $this->_conexion->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
            // Las cadenas vacias son convertidas en NULL
            $this->_conexion->setAttribute(\PDO::ATTR_ORACLE_NULLS, \PDO::NULL_EMPTY_STRING);
            // Las cadenas vacias son convertidas en NULL
            $this->_conexion->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        } catch (\PDOException $e) {

            throw new \ErrorException(get_class($this) . ' | Error al conectar con la BD. La BD dio el siguiente error: ' . utf8_encode($e->getMessage()),
                (int)$e->getCode()
            );

        }

        return $this;

    }

}