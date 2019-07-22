<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 06-15-18/11:16 AM
 * Version: 1.0
 *
 * Description: 
 */

namespace Vigoron\GestorDatos;

/**
 * Class GestorDB2
 * @package Vigoron\GestorDatos
 */
class GestorDB2 {

    const DRIVER = 'DRIVER={IBM DB2 ODBC DRIVER};';

    /**
     * @var array
     */
    private static $_instancias = array();
    /**
     * @var string
     */
    private $_instancia;
    /**
     * @var resource
     */
    protected $_conexion;
    /**
     * @var Sql|string
     */
    private $_sql;
    /**
     * @var string
     */
    private $_strQuery;
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
        'auto_cache'    => null,
        'depurar'       => false
    );
    /**
     * @var \Exception
     */
    private $_gestorExcepcion;

    /**
     * Retorna una instancia GestorDB2.
     *
     * Obtiene una solicitud para una instancia. Si encuentra la instancia dentro del array de $instancias la retorna.
     * De lo contrario crea una nueva instancia GestorDB2, la configura usando el array de configuracion, la guarda
     * en el array de instancias para entregarla a request futuros y retorna el nuevo objeto creado.
     *
     * @param string $instancia
     * @param array|null $configuracion
     * @return GestorDB2
     */
    public static function obtenerInstancia($instancia, $configuracion = null)
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
     * Termina todas las conexiones DB2 creadas siempre que hayan sido instanciadas.
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

    private function _desconectar()
    {
        if ($this->_conexion)
            db2_close($this->_conexion);
    }

    public function __destruct()
    {
        $this->_desconectar();
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
            'HOSTNAME'  => ($this->_config['servidor']) ? $this->_config['servidor'] : null,
            'PORT'      => ($this->_config['puerto'] ? $this->_config['puerto'] : null),
            'PROTOCOL'  => 'TCPIP',
            'DATABASE'  => ($this->_config['basedatos'] ? $this->_config['basedatos'] : null),
            'UID'       => ($this->_config['usuario'] ? $this->_config['usuario'] : null),
            'PWD'       => ($this->_config['contrasena'] ? $this->_config['contrasena'] : null)
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

            $this->_conexion = db2_connect($cadenaConexion,
                null,
                null,
                array(
                    'DB2_ATTR_CASE' => DB2_CASE_UPPER
                )
            );

            if (!$this->_conexion)
                throw new \Exception(db2_conn_errormsg(), db2_conn_error());

        } catch (\Exception $e) {

            throw new \ErrorException(get_class($this) . ' | Error al conectar con la BD. La BD dio el siguiente error: ' . utf8_encode($e->getMessage()),
                (int)$e->getCode()
            );

        }

        return $this;
    }

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
        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        $datos = array();

        try {

            $sql = $this->_sql;

            $prepStatement = $sql->getPrepare(3);
            $this->_sql = db2_prepare($this->_conexion, $prepStatement);

            $campos_parametrizados = $sql->getParams();
            if (!empty($campos_parametrizados)) {
                foreach ($campos_parametrizados as &$v)
                    $v = $v['value'];

                $resultado = db2_execute($this->_sql, $campos_parametrizados);
            } else {
                foreach ($campos_parametrizados as &$v)
                    $v = $v['value'];

                $resultado = db2_execute($this->_sql);
            }

            if (!$resultado)
                throw new \Exception('Error al ejecutar la consulta: ' . db2_stmt_errormsg(), db2_stmt_error());


            if ($campo_llave) {
                while ($fila = db2_fetch_assoc($this->_sql)) {
                    if (isset($fila[$campo_llave]))
                        $datos[$fila[$campo_llave]] = $fila;
                }
            } else {
                while ($fila = db2_fetch_assoc($this->_sql)) {
                    $datos[] = $fila;
                }
            }

            db2_free_result($this->_sql);

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($datos);
            }

            return $datos;

        } catch (\Exception $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

    }

    /**
     * Ejecuta una consulta preparada SELECT y retorna la columa solicitada.
     *
     * @param int $numeroColumna
     * @param string|null $campo_llave
     * @return array
     * @throws \ErrorException
     * @throws \Exception
     */
    public function obtenerColumna($numeroColumna = 0, $campo_llave = null)
    {
        $datos = $this->obtener($campo_llave);

        if (empty($datos))
            return null;

        try {

            foreach ($datos as $k => &$fila) {
                $llaves = array_fill(0, count($fila), null);
                $fila = array_combine(array_keys($llaves), $fila);

                if (!isset($fila[$numeroColumna]))
                    throw new \Exception('El indice solicitado no se encuentra disponible');
                else
                    $fila = $fila[$numeroColumna];
            }

            return $datos;

        } catch (\Exception $e) {
            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }

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
        $datos = $this->obtener();

        if (empty($datos))
            return array();

        return current($datos);
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
        $fila = $this->obtenerFila();

        if (empty($fila))
            return null;

        $llaves = array_fill(0, count($fila), null);
        $fila = array_combine(array_keys($llaves), $fila);

        try {
            if (!isset($fila[$numeroColumna]))
                throw new \Exception('El indice solicitado no se encuentra disponible');
            else
                return $fila[$numeroColumna];
        } catch (\Exception $e) {
            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }
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
     */
    public function ejecutar()
    {
        if (!$this->_conexion)
            $this->conectar();

        // Verifica que existe una consulta a ejecutar y que sea una instancia de PDOStatement
        if (!$this->_sql)
            throw new \Exception(get_class($this) . ' | La consulta SQL a ejecutar esta vacia');

        try {

            $sql = $this->_sql;

            $prepStatement = $sql->getPrepare(3);
            $this->_sql = db2_prepare($this->_conexion, $prepStatement);

            $campos_parametrizados = $sql->getParams();
            if (!empty($campos_parametrizados)) {
                foreach ($campos_parametrizados as &$v)
                    $v = $v['value'];

                $resultado = db2_execute($this->_sql, $campos_parametrizados);
            } else {
                foreach ($campos_parametrizados as &$v)
                    $v = $v['value'];

                $resultado = db2_execute($this->_sql);
            }

            if (!$resultado)
                throw new \Exception('Error al ejecutar la consulta: ' . db2_stmt_errormsg(), db2_stmt_error());


            db2_free_result($this->_sql);

            if ($this->_config['depurar']) {
                $this->_mostrarConsultaEjecutada();
                self::_mostrarEnPantalla($resultado);
            }

            return $resultado;

        } catch (\Exception $e) {

            $this->_gestorExcepcion = $e;

            $q = $this->_obtenerConsultaDefectuosa();
            throw new \ErrorException(get_class($this) . ' | La consulta [ '.$q.' ] dío el siguiente error: ' . $e->getMessage());
        }
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

        return db2_num_rows($this->_sql);
    }

    /**
     * Retorna el ID del ultimo registro insertado por la ultima consulta.
     *
     * Retorna el valor del ultimo ID insertado por la ultima consulta, siempre que la tabla
     * tenga un campo PRIMARY_KEY.
     *
     * @return string
     * @throws \ErrorException
     */
    public function obtenerUltimoId()
    {
        if (!$this->_conexion)
            $this->conectar();

        return db2_last_insert_id($this->_conexion);
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
     *
     * @return $this
     * @throws \Exception
     */
    public function sql(Sql $sql)
    {
        if (!$this->_conexion)
            $this->conectar();

        if ($sql instanceof Sql) {
            $this->_sql = $sql;
            $this->_strQuery  = $sql->getProceced();
        } else
            throw new \Exception(get_class($this) . ' | La consulta SQL debe ser una instáncia de la clase SQL.');

        return $this;
    }

    /**
     * En caso de fallar una consulta, retorna la consulta en un formato legible.
     *
     * @return string
     */
    private function _obtenerConsultaDefectuosa()
    {
        return $this->_strQuery;
    }

    private function _mostrarConsultaEjecutada()
    {
        $q = $this->_obtenerConsultaDefectuosa();

        echo "\r\n<div class=\"util_depurar_var\">\r\n";
        var_export($q);
        echo "</div>\r\n";

    }

    private static function _mostrarEnPantalla($valor)
    {
        echo "\r\n<div class=\"util_depurar_var\">\r\n";
        echo "<pre>\r\n";
        var_export($valor);
        echo "\r\n</pre>\r\n";
        echo "</div>\r\n";
    }

    /*
     * EXCEPCIONES
     */

    public function obtenerUltimoError()
    {
        return $this->_gestorExcepcion;
    }

}