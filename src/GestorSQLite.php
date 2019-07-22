<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 06-15-16/09:24 AM
 * Version: 1.0
 *
 * Description: 
 */

namespace Vigoron\GestorDatos;
/**
 * Class GestorSQLite
 * @package Vigoron\GestorDatos
 */
class GestorSQLite extends GestorDB {

    const DRIVER = 'sqlite:';

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
        // El elemento 'servidor' debe traer el path del archivo sqlite.
        // Si el archivo existe se conecta. Si no existe lo crea en la ruta especificada.
        // Ej: bd.sqlite3
        $cadenaConexion = array(
            'host'      => ($this->_config['servidor']) ? $this->_config['servidor'] : null
        );

        $cadenaConexion = self::DRIVER . current($cadenaConexion);

        // Realizamos conexion con la BD
        try {

            $this->_conexion = new \PDO($cadenaConexion,
                null,
                null,
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

        } catch (\PDOException $e) {

            throw new \ErrorException(get_class($this) . ' | Error al conectar con la BD. La BD dio el siguiente error: ' . utf8_encode($e->getMessage()),
                (int)$e->getCode()
            );

        }

        return $this;

    }

}