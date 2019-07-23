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
 * Class GestorDB
 * @package fmichell\GestorDatos
 */
abstract class GestorDB {

    /**
     * @var array
     */
    private static $_instancias = array();
    /**
     * @var string
     */
    private $_instancia;
    /**
     * @var \PDO
     */
    protected $_conexion;
    /**
     * @var Sql
     */
    private $_sql;
    /**
     * @var array
     */
    protected $_config = array(
        'servidor'      => null,
        'puerto'        => null, // 3306
        'usuario'       => null,
        'contrasena'    => null,
        'basedatos'     => null,
        'persistente'   => false,
        'charset'       => null, // utf8, latin1, ascii
        'auto_cache'    => false,
        'depurar'       => false
    );
    /**
     * @var \Exception
     */
    private $_gestorExcepcion;

    /*
     * Propiedades de cache
     */

    // Constante del tiempo de vida por defecto cuando no sea directamente definido.
    const CACHE_TIEMPO = 3600;

    /**
     * @var \GestorCache
     */
    private $_gestorCache;
    /**
     * @var string
     */
    private $_cacheLlave = '';
    /**
     * @var bool
     */
    private $_cacheAutoLlave = true;
    /**
     * Defecto: 3600 segundos = 1 hora
     * @var int
     */
    private $_cacheTiempo = self::CACHE_TIEMPO;
    /**
     * @var bool
     */
    private $_cacheActiva = true;
    /**
     * @var bool
     */
    private $_cacheSobreescribir = false;


    /**
     * Retorna una instancia GestorMySQL.
     *
     * Obtiene una solicitud para una instancia. Si encuentra la instancia dentro del array de $instancias la retorna.
     * De lo contrario crea una nueva instancia GestorMySQL, la configura usando el array de configuracion, la guarda
     * en el array de instancias para entregarla a request futuros y retorna el nuevo objeto creado.
     *
     * @param string $instancia
     * @param array|null $configuracion
     * @return GestorMySQL
     */
    public static function obtenerInstancia($instancia = 'produccion', $configuracion = null)
    {
        if (isset(self::$_instancias[$instancia])) {
            return self::$_instancias[$instancia];
        } else {
            self::$_instancias[$instancia] = new static($instancia);
            if ($configuracion)
                self::$_instancias[$instancia]->configurar($configuracion);

            return self::$_instancias[$instancia];
        }
    }

    /**
     * Termina todas las conexiones MySQL creadas siempre que hayan sido instanciadas.
     */
    public static function terminarConexiones()
    {
        if (!empty(self::$_instancias)) {
            foreach (self::$_instancias as $instancia) {

                if ($instancia->_conexion instanceof \PDO)
                    $instancia->desconectar();

            }
        }
    }

    /**
     * @param string $instancia
     */
    private function __construct($instancia)
    {
        $this->_instancia = $instancia;
    }

    /**
     * @throws \Exception
     */
    public function __clone()
    {
        throw new \Exception(get_class($this) . ' | No puedes clonar esta instancia de clase.');
    }

    /**
     * @throws \Exception
     */
    public function __wakeup()
    {
        throw new \Exception(get_class($this) . ' | No puedes deserializar esta instancia de clase.');
    }

    /**
     * @throws \Exception
     */
    public function __sleep()
    {
        throw new \Exception(get_class($this) . ' | No puedes serializar esta instancia de clase.');
    }

    public function __destruct()
    {
        $this->desconectar();
    }

    public function desconectar()
    {
        $this->_conexion = null;
        $this->_sql = null;
    }

    /**
     * Registra los datos de conexion (host, usuario, contrasena, etc).
     *
     * @param array $configuracion
     * @return $this
     */
    public function configurar(array $configuracion)
    {
        $this->_config = array_merge($this->_config, $configuracion);
        return $this;
    }

    /**
     * Declaración de metodo abstracto de conexion que deberá ser implementado por las clases hijas.
     *
     * @param array|null $configuracion
     *
     * @return mixed
     */
    abstract public function conectar($configuracion = null);

    /**
     * Ejecuta una consulta preparada SELECT y retorna un array con los resultados.
     *
     * Ejecuta una consulta preparada SELECT y retorna un array con los resultados. Si el campo_llave es null
     * el array de resultados se indexara natutalmente (0,1,2,3...). Si el campo_llave es distinto de null, el
     * array de resultados se indexara usando el valor del campo que se haya pasado como campo llave, es decir,
     * que el campo_llave debe existir en el resultado de la consulta.
     *
     * @param string|null $campo_llave
     * @return array
     * @throws \ErrorException
     * @throws \Exception
     */
    public function obtener($campo_llave = null)
    {
        if (!$this->_conexion)
            $this->conectar();

        // Verifica que existe una consulta a ejecutar y que sea una instancia de PDOStatement
        if (!$this->_sql or !$this->_sql instanceof \PDOStatement)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        // Verificamos cache
        $dataCache = $this->_obtenerCache($this->_cacheLlave.$campo_llave);
        if ($dataCache !== GestorCache::CACHE_NOT_FOUND) {
            $this->_setCacheDefautValues();
            return $dataCache;
        }

        $datos = array();

        try {
            $this->_sql->execute();
            $this->_sql->setFetchMode(\PDO::FETCH_ASSOC);
            if ($campo_llave) {
                while ($fila = $this->_sql->fetch()) {
                    if (isset($fila[$campo_llave]))
                        $datos[$fila[$campo_llave]] = $fila;
                }
            } else {
                $datos = $this->_sql->fetchAll();
            }

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($datos);
            }

        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_guardarCache($datos);

        return $datos;
    }

    /**
     * Ejecuta una consulta preparada SELECT y retorna la columna solicitada.
     *
     * @param int $numeroColumna
     * @param string|null $campo_llave
     * @return array
     * @throws \ErrorException
     * @throws \Exception
     */
    public function obtenerColumna($numeroColumna = 0, $campo_llave = null)
    {
        if (!$this->_conexion)
            $this->conectar();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        // Verificamos cache
        $dataCache = $this->_obtenerCache($this->_cacheLlave.'Col'.$numeroColumna);
        if ($dataCache !== GestorCache::CACHE_NOT_FOUND) {
            $this->_setCacheDefautValues();
            return $dataCache;
        }

        $columna = array();

        try {
            $this->_sql->execute();
            $this->_sql->setFetchMode(\PDO::FETCH_NUM);
            if (!is_null($campo_llave)) {
                while ($fila = $this->_sql->fetch()) {
                    if (isset($fila[$campo_llave]))
                        $columna[$fila[$campo_llave]] = $fila[$numeroColumna];
                }
            } else {
                while ($campo = $this->_sql->fetchColumn($numeroColumna)) {
                    $columna[] = $campo;
                }
            }

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($columna);
            }

        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_guardarCache($columna);

        return $columna;
    }

    /**
     * Ejecuta una consulta preparada SELECT y retorna un array con la primera fila del resultado.
     *
     * @return array
     * @throws \ErrorException
     * @throws \Exception
     */
    public function obtenerFila()
    {
        if (!$this->_conexion)
            $this->conectar();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        // Verificamos cache
        $dataCache = $this->_obtenerCache($this->_cacheLlave.'Row');
        if ($dataCache !== GestorCache::CACHE_NOT_FOUND) {
            $this->_setCacheDefautValues();
            return $dataCache;
        }

        $datos = array();

        try {
            $this->_sql->execute();
            $this->_sql->setFetchMode(\PDO::FETCH_ASSOC);
            $fila = $this->_sql->fetch();
            if ($fila)
                $datos = $fila;

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($datos);
            }

        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_guardarCache($datos);

        return $datos;
    }

    /**
     * Ejecuta una consulta preparada SELECT y retorna el primer valor de la columa solicitada.
     *
     * @param int $numeroColumna
     *
     * @return bool|void
     * @throws \ErrorException
     * @throws \Exception
     */
    public function obtenerCampo($numeroColumna = 0)
    {
        if (!$this->_conexion)
            $this->conectar();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        // Verificamos cache
        $dataCache = $this->_obtenerCache($this->_cacheLlave.'Campo'.$numeroColumna);
        if ($dataCache !== GestorCache::CACHE_NOT_FOUND) {
            $this->_setCacheDefautValues();
            return $dataCache;
        }

        try {
            $this->_sql->execute();
            $columna = $this->_sql->fetchColumn($numeroColumna);

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($columna);
            }

        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_guardarCache($columna);

        return $columna;
    }

    /**
     * Ejecuta una consulta preparada SELECT y retorna un array con el resultado, agrupando por el $campo_grupo.
     *
     * Ejecuta una consulta preparada SELECT y retorna un array con el resultado, agrupando los registros
     * por el $campo_grupo. Si el $campo_llave es distinto de null, indexara cada grupo usando el valor del campo
     * pasado como $campo_llave.
     *
     * @param string $campo_grupo
     * @param string|null $campo_llave
     *
     * @return array
     * @throws \ErrorException
     */
    public function obtenerGrupos($campo_grupo, $campo_llave = null)
    {
        $resultado = $this->obtener($campo_llave);

        $datos = array();

        foreach($resultado as $llave => $fila) {
            if(isset($fila[$campo_grupo])) {
                $datos[$fila[$campo_grupo]][$llave] = $fila;
            }
        }

        return $datos;
    }

    /**
     * Ejecuta una consulta preparada INSERT, UPDATE, DELETE.
     *
     * Método por defecto para ejecución de consultas que afecten la BD. Siempre deberá usarse este método cuando se
     * ejecuten consultas independientes.
     *
     * @return bool
     * @throws \ErrorException
     * @throws \Exception
     */
    public function ejecutar()
    {
        // Si la propiedad _sql es una instancia de Sql, significa que el query deberá ejecutarse en Bash, por lo que se
        // pasa la ejecución al método _ejecutarEnBash.
        if ($this->_sql instanceof Sql) {
            return $this->_ejecutarEnBash();
        }

        if (!$this->_conexion)
            $this->conectar();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        try {
            $resultado = $this->_sql->execute();

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($resultado);
            }

        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            if ( in_array($e->errorInfo[1], array(
                    1062, // Controlando error de llave primaria duplicada
                    1451, // Controlando error de integridad relacional: Eliminando registro relacionado.
                    1452  // Controlando error de integridad relacional: Insertando o actualizando registro sin registro relacionado.
                ))) {
                return false;
            } else {
                $q = $this->_obtenerConsultaDefectuosa();
                throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
            }
        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_setCacheDefautValues();

        return $resultado;
    }

    /**
     * Ejecuta una consulta preparada INSERT, UPDATE o DELETE en Bach. Este método deberá usarse cuando
     * se quiera ejecutar una consulta muchas veces con valores distintos en cada iteración del bucle.
     *
     * Este método solo funciona cuando la propiedad _sql es una instancia de la clase Sql. Los parametros se asignan
     * por medio del método Sql->setPrepareQueryValues (ver documentación en la clase Sql).
     *
     * @return bool
     * @throws \ErrorException
     * @throws \Exception
     */
    private function _ejecutarEnBash()
    {
        if (!$this->_conexion)
            $this->conectar();

        $sql = $this->_sql;

        $prepStatement = $sql->getPrepare(2);
        $this->_sql = $this->_conexion->prepare($prepStatement);

        $campos_parametrizados = $sql->getParams();
        $arrayValores = $sql->getBashParams();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        try {
            foreach ($arrayValores as $k => $valores) {

                foreach ($valores as $indice => $valor) {
                    $parametro = $campos_parametrizados[ $indice ]['param'];
                    $this->_sql->bindValue($parametro, $valor);
                }

                $resultado = $this->_sql->execute();
                unset($arrayValores[$k]);

            }
        } catch (\PDOException $e) {

            $this->_gestorExcepcion = $e;

            if ( in_array($e->errorInfo[1], array(
                1062, // Controlando error de llave primaria duplicada
                1451, // Controlando error de integridad relacional: Eliminando registro relacionado.
                1452  // Controlando error de integridad relacional: Insertando o actualizando registro sin registro relacionado.
            ))) {
                return false;
            } else {
                $q = $this->_obtenerConsultaDefectuosa();
                throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
            }

        }

        $this->_sql->closeCursor();
        $this->_sql = null;

        $this->_setCacheDefautValues();

        return $resultado;
    }

    /**
     * Retorna la cantidad de registros afectados por la ultima consulta.
     *
     * @return int
     * @throws \Exception
     */
    public function obtenerAfectados()
    {
        if (!$this->_conexion)
            $this->conectar();

        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        $afectados = $this->_sql->rowCount();

        return $afectados;
    }

    /**
     * Retorna el ID del ultimo registro insertado por la ultima consulta.
     *
     * Retorna el valor del ultimo ID insertado por la ultima consulta, siempre que la tabla
     * tenga un campo PRIMARY_KEY.
     *
     * @return string
     */
    public function obtenerUltimoId()
    {
        if (!$this->_conexion)
            $this->conectar();

        $ultimoId = $this->_conexion->lastInsertId();

        return $ultimoId;
    }

    /**
     * Prepara una consulta SQL para su ejecucion.
     *
     * En caso que el parametro Sql sea un objeto Sql, el método verificará sí la bandera execAsBash es true. De ser así,
     * seteará la propiedad _sql con el objeto Sql terminará la ejecución, para que la consulta sea preparada y ejecutada
     * por el método ejecutarEnBash.
     *
     * En caso que el parametro Sql sea un objeto Sql pero la bandera execAsBash sea false, el método prepará la consulta
     * para su ejecución.
     *
     * Sí el parámetro Sql no es un objeto Sql, el metodo arrojará una excepcion.
     *
     * @param Sql $sql
     * @param string $paramPlaceholder
     *
     * @return $this
     * @throws \Exception
     */
    public function sql(Sql $sql, $paramPlaceholder = 'named')
    {
        if (!$this->_conexion)
            $this->conectar();

        if ($sql instanceof Sql) {

            // Definimos la consulta a ejecutar como posible llave de cache siempre que no haya sido definida antes.
            if ($this->_cacheAutoLlave == true) {
                $sql->getPrepare();
                $this->_cacheLlave = $sql->getProceced();
            }

            // Si la consulta será ejecutada como consulta preparada no continuamos con el proceso
            // pues los parametros serán enlazados al momento de ser ejecutada.
            if ($sql->execAsBash()) {
                $this->_sql = $sql;

                return $this;
            }

            // Si la bandera execAsBash es false, se prepara la consulta para su posterior ejecución.
            if ($paramPlaceholder == 'named')
                $prepStatement = $sql->getPrepare(2);
            else
                $prepStatement = $sql->getPrepare(3);

            $this->_sql = $this->_conexion->prepare($prepStatement);

            $campos_parametrizados = $sql->getParams();
        } else
            throw new \Exception(get_class($this) . ' | La consulta SQL debe ser una instáncia de la clase SQL.');

        // Enlazamos parámetros y valores.
        if (!empty($campos_parametrizados)) {
            $i = 1;
            foreach($campos_parametrizados as $parametro) {

                if ($paramPlaceholder == 'named')
                    $placeholder = $parametro['param'];
                else
                    $placeholder = $i;

                if (isset($parametro['type']))
                    $this->_sql->bindValue($placeholder, $parametro['value'], self::_getDataType($parametro['type']));
                else
                    $this->_sql->bindValue($placeholder, $parametro['value']);

                $i++;
            }
        }

        return $this;
    }

    /**
     * Escapa el valor de un parametro segun el tipo de dato pasado.
     *
     * @param $type
     * @return int
     */
    private static function _getDataType($type)
    {
        if ($type == 'bool') {
            return \PDO::PARAM_BOOL;
        } elseif ($type == 'int') {
            return \PDO::PARAM_INT;
        } else {
            return \PDO::PARAM_STR;
        }
    }

    /**
     * En caso de fallar una consulta, retorna la consulta en un formato legible.
     *
     * @return string
     */
    private function _obtenerConsultaDefectuosa()
    {
        ob_start();
        $this->_sql->debugDumpParams();
        $q = ob_get_contents();
        ob_clean();

        $pos = strpos($q, 'Params:');
        if ($pos !== false)
            $q = substr($q, 0, $pos);

        return $q;
    }

    private function _mostrarConsultaEjecutada()
    {
        $q = $this->_obtenerConsultaDefectuosa();

        echo "\r\n<div style=\"border: solid 2px #CCC; padding: 5px; margin-bottom: 1em;\">\r\n";
        var_export($q);
        echo "</div>\r\n";
    }

    private static function _mostrarEnPantalla($valor)
    {
        echo "\r\n<div style=\"border: solid 2px #CCC; padding: 5px; margin-bottom: 1em;\">\r\n";
        echo "<pre>\r\n";
        var_export($valor);
        echo "\r\n</pre>\r\n";
        echo "</div>\r\n";
    }

    /*
     * TRANSACCIONES
     */

    /**
     * Inicia una transaccion.
     *
     * @return bool
     */
    public function iniciarTransaccion()
    {
        if (!$this->_conexion)
            $this->conectar();

        return $this->_conexion->beginTransaction();
    }

    /**
     * Ejecuta todas las consultas desde el ultimo inicio de transaccion y cierra la transaccion.
     *
     * @return bool
     */
    public function commitTransaccion()
    {
        if (!$this->_conexion)
            $this->conectar();

        return $this->_conexion->commit();
    }

    /**
     * Roolback de todas las consultas desde el ultimo inicio de transaccion y cierra la transaccion.
     *
     * @return bool
     */
    public function revertirTransaccion()
    {
        if (!$this->_conexion)
            $this->conectar();

        return $this->_conexion->rollBack();
    }

    /*
     * EXCEPCIONES
     */

    public function obtenerUltimoError()
    {
        return $this->_gestorExcepcion;
    }

    /*
     * CACHE
     */

    /**
     * @param GestorCache $gestorCache
     */
    public function setCache(GestorCache $gestorCache)
    {
        $this->_gestorCache = $gestorCache;
    }

    /**
     * @return \GestorCache
     * @throws \Exception
     */
    public function getCache()
    {
        return $this->_gestorCache;
    }

    /**
     * @return bool
     * @throws \Exception
     */
    private function _validarCache()
    {
        if ($this->_config['auto_cache']) {
            if ($this->_gestorCache instanceof GestorCache)
                return true;
            else
                throw new \Exception(get_class($this) . ' | GestorCache no ha sido definido.');
        } else {
            return false;
        }
    }

    /**
     * Reinicia las propiedades relacionadas con cache a su estado original.
     */
    private function _setCacheDefautValues()
    {
        $this->_cacheActiva = true;
        $this->_cacheSobreescribir = false;
        $this->_cacheLlave = '';
        $this->_cacheAutoLlave = true;
        $this->_cacheTiempo = self::CACHE_TIEMPO;
    }

    /**
     * Implementa cache a la proxima consulta.
     *
     * @param \GestorCache $tiempo
     * @param null|string $llave
     *
     * @throws \Exception
     */
    public function cache($tiempo = SELF::CACHE_TIEMPO, $llave = null)
    {
        if (!$this->_validarCache())
            return;

        $this->_cacheTiempo = $tiempo;
        $this->_cacheLlave  = $llave;
        $this->_cacheAutoLlave = false;
    }

    /**
     * Permite definir el tiempo de vida de una llave de autocache.
     *
     * @param \GestorCache $tiempo
     *
     * @throws \Exception
     */
    public function setTiempoAutocache($tiempo = SELF::CACHE_TIEMPO)
    {
        if (!$this->_validarCache())
            return;

        $this->_cacheTiempo = $tiempo;
    }

    /**
     * Desactiva el gestor cache para la siguiente consulta sin sobrescribir los datos en cache.
     */
    public function desactivarCache()
    {
        if (!$this->_validarCache())
            return;

        $this->_cacheActiva = false;
    }

    /**
     * Forza la sobreescritura de la cache para la siguiente consulta.
     */
    public function sobrescribirCache()
    {
        if (!$this->_validarCache())
            return;

        $this->_cacheSobreescribir = true;
    }

    /**
     * Retorna los datos guardados en cache.
     *
     * @param string $llave
     *
     * @return bool|void
     * @throws \Exception
     */
    private function _obtenerCache($llave)
    {
        if (!$this->_validarCache() or !$this->_cacheActiva)
            return false;

        // Si esta activa la bandera de autoLlave, entonces aplicamos md5 a la consulta para generar la llave;
        // Si esta inactiva la bandera de autoLlave, validamos que la llave esté definida, de lo contrario disparamos excepción.
        if ($this->_cacheAutoLlave == true)
            $this->_cacheLlave = md5($llave);
        elseif (!$this->_cacheAutoLlave and empty($this->_cacheLlave))
            throw new \Exception(get_class($this) . ' | Llave de cache indefinida.');

        if ($this->_cacheSobreescribir)
            return false;

        $resultado = $this->_gestorCache->obtener($this->_cacheLlave);

        return $resultado;
    }

    /**
     * Guarda datos en cache.
     *
     * @param mixed $datos
     *
     * @throws \Exception
     */
    private function _guardarCache($datos)
    {
        if ($this->_validarCache() and $this->_cacheActiva) {
            $this->_gestorCache->guardar($this->_cacheLlave, $datos, $this->_cacheTiempo);
        }

        $this->_setCacheDefautValues();

        return;
    }

}