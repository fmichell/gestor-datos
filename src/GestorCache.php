<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 01-19-17/09:04 PM
 *
 * Description: 
 */

namespace Vigoron\GestorDatos;
/**
 * Class GestorCache
 * @package Vigoron\GestorDatos
 */
class GestorCache {

    const CACHE = CACHE;

    const ANTECEDENTE_LLAVE = '';

//    const CACHE_NOT_FOUND = 'cache_not_found';
    const CACHE_NOT_FOUND = false;

    /**
     * @var array
     */
    private static $_instancias = array();
    /**
     * @var string
     */
    private $_instancia;
    /**
     * @var string
     */
    private $_app;
    /**
     * @var string
     */
    private $_cuentaId;
    /**
     * @var \Memcached|\Memcache
     */
    public $mc;

    private $_debug = false;

    /**
     * Retorna una instancia GestorCache.
     *
     * Retorna una instancia del GestorCache. Si la instancia ya existe la reutiliza; si no existe
     * crea una nueva instancia y se conecta al servidor determinado en el array de configuracion.
     * El GestorCache soporta conexiones a servidores multiples. Distinga cada servidor con un nombre
     * de instancia distinta.
     * En caso que no se valla a usar cache, creara una instancia pero sin conexion a ningun servidor.
     *
     * @param string $instancia
     * @param array|null $configuracion
     * @return GestorCache
     */
    static public function obtenerInstancia($instancia = 'produccion', $configuracion = null)
    {
        if (!self::CACHE)
            return self::$_instancias[$instancia] = new self($instancia);

        if (isset(self::$_instancias[$instancia])) {
            return self::$_instancias[$instancia];
        } else {
            self::$_instancias[$instancia] = new self($instancia);
            if ($configuracion)
                self::$_instancias[$instancia]->configurar($configuracion);

            return self::$_instancias[$instancia];
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

    /**
     * Configura e inicia la conexion con el servidor de cache.
     *
     * Configura la conexion con el servidor de cache y crea un objeto
     * memcache o memcached en dependencia de cual este disponible en el servidor.
     *
     * @param array $configuracion
     * @return $this
     */
    public function configurar(array $configuracion)
    {
        try {
            if ( extension_loaded('memcache') and $configuracion['gestor'] == 'memcache') {

                $mc = new GestorCacheMemcache($configuracion);
                $this->mc = $mc->obtenerGestor();

            } elseif ( extension_loaded('memcached') and $configuracion['gestor'] == 'memcached') {

                $mc = new GestorCacheMemcached($configuracion);
                $this->mc = $mc->obtenerGestor();

            } else {

                throw new \ErrorException(get_class($this) . ' | Imposible conectar con el servidor de cache');

            }

            $this->_app = $configuracion['app'];
            $this->_cuentaId = $configuracion['cuenta_id'];

            if (isset($configuracion['depurar']))
                $this->_debug = $configuracion['depurar'];

            return $this;
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Guarda los datos pasados en una llave de cache.
     *
     * Si no hay conexion de cache siempre retorna true para hacer creer al sistema que se
     * guardaron los datos.
     *
     * @param string $llave
     * @param mixed $datos
     * @param int $expiracion
     * @return bool
     */
    public function guardar($llave, $datos, $expiracion = 0)
    {
        if (!self::CACHE)
            return true;

        $llave = $this->obtenerPrefijo() . $llave;

        if ($this->_debug == true) {
            ob_start();
            var_export($datos);
            $d = ob_get_contents();
            ob_clean();

            self::_depurar_var("GUARDAR<br>LLAVE: " . $llave . "<br>EXPIRACION: " . $expiracion . "<br>DATOS: <br>" . $d);
        }

        return $this->mc->set($llave, $datos, $expiracion);
    }

    /**
     * Obtiene datos de una llave de cache.
     *
     * Si no hay conexion de cache siempre retorna false para hacer creer al sistema que
     * no existen datos guardados, y asi forzar a la BD a hacer la consulta.
     *
     * @param string $llave
     * @return mixed
     */
    public function obtener($llave)
    {
        if (!self::CACHE)
            return false;

        $llave = $this->obtenerPrefijo() . $llave;

        $datos = $this->mc->get($llave);

        // TODO Nota importante al actualizar a php7
        /*
         * Cuando se almacena en cache el valor bool false, el código de confunde y piensa que el false corresponde
         * a que la llave no ha sido creada.
         * Para solucionar esto, si el valor obtenido es false, consultamos el código de respuesta de Memcached. Si el
         * valor del código es 00 significa que es un falso positivo, es decir, que el false es un valor real.
         * Si el código de respuesta es 16, significa que el false corresponde a llave no encontrada.
         *
         * Cuando el false sea un falso real (llave no encontrada), vamos a retornar el string "cache_not_found".
         * Este será el identificador de llave no encontrada.
         *
         * OJO!!!!
         * Para obtener el codigo de respuesta se usa el metodo Memcached::getResultCode(). Si se actualiza PHP hay
         * que validar que este disponible en la nueva version de Memcached.
         */

        if ($datos === false and $this->obtenerCodigo() == 16)
            $datos = self::CACHE_NOT_FOUND;

        if ($this->_debug == true) {
            ob_start();
            var_export($datos);
            $d = ob_get_contents();
            ob_clean();

            self::_depurar_var("OBTENER<br>LLAVE: " . $llave . "<br>DATOS: <br>" . $d);
        }

        return $datos;
    }

    /**
     * Retorna el codigo del resultado de la ultima operacion.
     *
     * Si no hay conexion de cache siempre retorna 0.
     *
     * @return int
     */
    public function obtenerCodigo()
    {
        if (!self::CACHE)
            return 0;

        if ($this->mc instanceof \Memcached)
            return $this->mc->getResultCode();
        else
            return 0;
    }

    /**
     * Elimina una llave de cache.
     *
     * Si no hay conexion de cache siempre retornamos true para hacer creer al sistema que la llave
     * fue eliminada.
     *
     * @param string $llave
     * @return mixed
     */
    public function eliminar($llave)
    {
        if (!self::CACHE)
            return true;

        $llave = $this->obtenerPrefijo() . $llave;

        if ($this->_debug == true) {
            self::_depurar_var("ELIMINAR<br>LLAVE: " . $llave);
        }

        return $this->mc->delete($llave);
    }

    /**
     * Retorna un listado de todas las llaves de cache almacenadas en los servidores.
     *
     * @return array
     */
    public function obtenerLlaves()
    {
        if (!self::CACHE or $this->mc instanceof \Memcache)
            return array();

        $llaves = $this->mc->getAllKeys();
        return $llaves;
    }

    /**
     * Retorna el prefijo de la llave de cache.
     *
     * @return string
     */
    public function obtenerPrefijo()
    {
        return self::ANTECEDENTE_LLAVE . $this->_app . '_' . $this->_cuentaId . '_';
    }

    /**
     * @param $valor
     * @param null $tiempo_inicial
     */
    private static function _depurar_var($valor)
    {
        echo "\r\n<div style=\"border: solid 2px #CCC; padding: 5px; margin-bottom: 1em;\">\r\n";
        echo "<pre>\r\n";
        var_export($valor);
        echo "\r\n</pre>\r\n";
        echo "</div>\r\n";
    }

}