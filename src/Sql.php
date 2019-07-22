<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 08-10-16/12:58 PM
 *
 * Description:
 */

namespace Vigoron\GestorDatos;
/**
 * Class Sql
 * @package Vigoron\GestorDatos
 */
class Sql {

    /**
     * @var string
     */
    private $_queryString = '';
    /**
     * @var string|null
     */
    private $_statement = null;
    /**
     * @var string|null
     */
    private $_statement_attributes = null;
    /**
     * @var array
     */
    private $_subStatement = array();
    /**
     * @var array|null
     */
    private $_fields = null;
    /**
     * @var string|null
     */
    private $_table = null;
    /**
     * @var array
     */
    private $_join = array();
    /**
     * @var array
     */
    private $_where = array();
    /**
     * @var array|null
     */
    private $_group = null;
    /**
     * @var array
     */
    private $_having = array();
    /**
     * @var array
     */
    private $_orderBy = array();
    /**
     * @var string|null
     */
    private $_limit = null;
    /**
     * @var array
     */
    private $_values = array();
    /**
     * @var string
     */
    private $_valueType = 'simple';
    /**
     * @var array
     */
    private $_params = array();
    /**
     * @var array
     */
    private $_bashParams = array();
    /**
     * @var int
     */
    private $_paramFormat = 1;
    /**
     * @var bool
     */
    private $_bashQuery = false;
    /**
     * @var int
     */
    private static $_id = 1;
    /**
     * @var int
     */
    private static $_parentId = 0;


    /**
     * Limpia to-dos los atributos preparandolos para obtener una nueva consulta.
     */
    public function clear()
    {
        $this->_queryString = '';
        $this->_statement = null;
        $this->_statement_attributes = null;
        $this->_fields = null;
        $this->_table = null;
        $this->_join = array();
        $this->_where = array();
        $this->_group = null;
        $this->_having = array();
        $this->_orderBy = array();
        $this->_limit = null;
        $this->_values = array();
        $this->_valueType = 'simple';
        $this->_params = array();
        $this->_bashParams = array();
        $this->_paramFormat = 1;
        $this->_bashQuery = false;

        self::$_id = 1;
        self::$_parentId = 0;
    }

    /**
     * Retorna el valor de la propiedad _bashQuery, que sirve para identificar si una consulta deberá ser ejecutada
     * como una query independiente o como query en Bash.
     *
     * @return bool
     */
    public function execAsBash()
    {
        return $this->_bashQuery;
    }

    /*
     * SELECT
     */

    /**
     * Establece el _statement como SELECT.
     *
     * Si la función recibe argumentos, los tomará como campos a consultar.
     * Si no, establecerá * como campos del select.
     *
     * @return $this
     */
    public function select()
    {
        $this->clear();

        $this->_statement = 'select';
        $this->_fields = func_get_args();

        if (empty($this->_fields))
            array_push($this->_fields, '*');

        return $this;
    }

    /*
     * FROM
     */

    /**
     * Establece la tabla.
     *
     * @param string $table
     */
    private function _setTable($table)
    {
        $this->_table = $table;
    }

    /**
     * Establece la tabla.
     *
     * @param string $table
     * @return $this
     */
    public function from($table)
    {
        $this->_setTable($table);

        return $this;
    }

    /*
     * JOIN
     */

    /**
     * Prepara un JOIN.
     *
     * @param string $joinTable
     * @param string $field1
     * @param string $field2
     * @param string|null $joinType
     * @return $this
     */
    public function join($joinTable, $field1, $field2, $joinType = null)
    {
        $joinType = trim(strtolower($joinType));

        if (in_array($joinType, array('inner','left','right')))
            $joinType.= ' join';
        else
            $joinType = 'join';

        $this->_join[] = array(
            'type'  => $joinType,
            'table' => $joinTable,
            'field1' => $field1,
            'field2' => $field2
        );

        return $this;
    }

    /**
     * Prepara un INNER JOIN.
     *
     * @param string $joinTable
     * @param string $field1
     * @param string $field2
     * @return $this
     */
    public function innerJoin($joinTable, $field1, $field2)
    {
        return $this->join($joinTable, $field1, $field2, 'inner');
    }

    /**
     * Prepara un RIGHT JOIN.
     *
     * @param string $joinTable
     * @param string $field1
     * @param string $field2
     * @return $this
     */
    public function rightJoin($joinTable, $field1, $field2)
    {
        return $this->join($joinTable, $field1, $field2, 'right');
    }

    /**
     * Prepara un LEFT JOIN.
     *
     * @param string $joinTable
     * @param string $field1
     * @param string $field2
     * @return $this
     */
    public function leftJoin($joinTable, $field1, $field2)
    {
        return $this->join($joinTable, $field1, $field2, 'left');
    }

    /*
     * WHERE
     */

    /**
     * Inicia un WHERE.
     *
     * @return $this
     */
    public function initWhere()
    {
        if (self::$_id == 1) {

            $condition = array(
                'id'        => self::$_id,
                'parentId'  => self::$_parentId,
            );

            $this->_where[] = $condition;

            self::$_parentId = self::$_id;
            self::$_id++;
        } else {

            $condition = array(
                'id'        => self::$_id++,
                'parentId'  => self::$_parentId,
            );

            $this->_where[] = $condition;

            $parent = end($this->_where);
            self::$_parentId = $parent['id'];
        }

        return $this;
    }

    /**
     * Agrega una regla incluyente (AND) al WHERE.
     *
     * @param string $field
     * @param string $operator
     * @param string $value
     * @return $this
     */
    public function andW($field, $operator, $value)
    {
        $condition = array(
            'condition' => 'and',
        );

        $condition = array_merge(
            $condition,
            $this->_prepareWhere($field, $operator, $value)
        );

        $this->_where[] = $condition;

        return $this;
    }

    /**
     * Agrega una regla excluyente (OR) al WHERE.
     *
     * @param string $field
     * @param string $operator
     * @param string $value
     * @return $this
     */
    public function orW($field, $operator, $value)
    {
        $condition = array(
            'condition' => 'or',
        );

        $condition = array_merge(
            $condition,
            $this->_prepareWhere($field, $operator, $value)
        );

        $this->_where[] = $condition;

        return $this;
    }

    /**
     * Prepara un WHERE.
     *
     * Prepara un campo where estableciendo su valor escapado, separando sus elementos y
     * asignándole un id y un parentId que luego serán usados para crear el arbol de where's.
     *
     * @param string $field
     * @param string $operator
     * @param string $value
     * @return array
     */
    private function _prepareWhere($field, $operator, $value)
    {
        $param = self::_prepareParam($field, $value);

        return array(
            'id'        => self::$_id++,
            'parentId'  => self::$_parentId,
            'field'     => $param['field'],
            'type'      => $param['type'],
            'operator'  => $operator,
            'value'     => $param['value']
        );
    }

    /**
     * Lleva el control del contador del parentId y genera el arbol de where's al cerrarse el último elemento.
     *
     * @return $this|bool
     */
    public function closeWhere()
    {
        foreach($this->_where as $where) {
            if (self::$_parentId == $where['id']) {
                self::$_parentId = $where['parentId'];
                break;
            }
        }

        if (self::$_parentId == 0) {
            $tree = self::_buildWhereTree($this->_where, 'parentId', 'id');
            $tree = current($tree);
            if (isset($tree['subStatement']) and !empty($tree['subStatement'])) {
                $this->_where = $tree['subStatement'];
                self::$_id = 1;
            } else
                return false;
        }

        return $this;
    }

    /**
     * Genera el arbol de where's.
     *
     * @param array $flat
     * @param string $pidKey
     * @param string|null $idKey
     * @return array
     */
    static private function _buildWhereTree($flat, $pidKey, $idKey = null)
    {
        $grouped = array();
        foreach ($flat as $sub){
            $grouped[$sub[$pidKey]][] = $sub;
        }

        $fnBuilder = function($siblings) use (&$fnBuilder, $grouped, $idKey) {
            foreach ($siblings as $k => $sibling) {
                $id = $sibling[$idKey];
                if(isset($grouped[$id])) {
                    $sibling['subStatement'] = $fnBuilder($grouped[$id]);
                }
                $siblings[$k] = $sibling;
            }

            return $siblings;
        };

        $tree = $fnBuilder($grouped[0]);

        return $tree;
    }

    /*
     * GROUP BY
     */

    /**
     * Establece los campos del GROUP BY.
     *
     * @return $this
     */
    public function groupBy()
    {
        $this->_group = func_get_args();

        return $this;
    }

    /*
     * HAVING
     */

    /**
     * Inicia un HAVING.
     *
     * @return $this
     */
    public function initHaving()
    {
        if (self::$_id == 1) {

            $condition = array(
                'id'        => self::$_id,
                'parentId'  => self::$_parentId,
            );

            $this->_having[] = $condition;

            self::$_parentId = self::$_id;
            self::$_id++;
        } else {

            $condition = array(
                'id'        => self::$_id++,
                'parentId'  => self::$_parentId,
            );

            $this->_having[] = $condition;

            $parent = end($this->_having);
            self::$_parentId = $parent['id'];
        }

        return $this;
    }

    /**
     * Agrega una regla incluyente (AND) al HAVING.
     *
     * @param string $field
     * @param string $operator
     * @param string $value
     * @return $this
     */
    public function andH($field, $operator, $value)
    {
        $condition = array(
            'condition' => 'and',
        );

        $condition = array_merge(
            $condition,
            $this->_prepareWhere($field, $operator, $value)
        );

        $this->_having[] = $condition;

        return $this;
    }

    /**
     * Agrega una regla excluyente (OR) al HAVING.
     *
     * @param string $field
     * @param string $operator
     * @param string $value
     * @return $this
     */
    public function orH($field, $operator, $value)
    {
        $condition = array(
            'condition' => 'or',
        );

        $condition = array_merge(
            $condition,
            $this->_prepareWhere($field, $operator, $value)
        );

        $this->_having[] = $condition;

        return $this;
    }

    /**
     * Lleva el control del contador del parentId y genera el arbol de having's al cerrarse el último elemento.
     *
     * @return $this|bool
     */
    public function closeHaving()
    {
        foreach($this->_having as $having) {
            if (self::$_parentId == $having['id']) {
                self::$_parentId = $having['parentId'];
                break;
            }
        }

        if (self::$_parentId == 0) {
            $tree = self::_buildWhereTree($this->_having, 'parentId', 'id');
            $tree = current($tree);
            if (isset($tree['subStatement']) and !empty($tree['subStatement'])) {
                $this->_having = $tree['subStatement'];
                self::$_id = 1;
            } else
                return false;
        }

        return $this;
    }

    /*
     * ORDER BY
     */

    /**
     * Agrega un campo de ordenamiento.
     *
     * @param string $field
     * @param string $order
     * @return $this
     */
    public function orderBy($field, $order = '+')
    {
        $this->_orderBy[] = array(
            'field' => $field,
            'order' => ($order == '-') ? 'DESC' : 'ASC'
        );

        return $this;
    }

    /*
     * LIMIT
     */

    /**
     * Agrega LIMIT.
     *
     * @param int $rows
     * @param int|null $position
     * @return $this
     */
    public function limit($rows, $position = null)
    {
        if ($position)
            $this->_limit = $position . ', ' . $rows;
        else
            $this->_limit = $rows;

        return $this;
    }

    /*
     * INSERT
     */

    /**
     * Inicia un INSERT.
     *
     * @param string $table
     * @return $this
     */
    public function insertInto($table)
    {
        $this->clear();

        $this->_statement = 'insert';
        $this->_setTable($table);

        return $this;
    }

    /**
     * Agrega un valor al INSERT o UPDATE.
     *
     * @param string $field
     * @param string $value
     * @return $this
     */
    public function setValue($field, $value)
    {
        $this->_values[] = self::_prepareParam($field, $value);

        return $this;
    }

    /**
     * Agrega multiples valores a un INSERT o UPDATE.
     *
     * Permite agregar multiples valores de una solo ves, para evitar hacer multibles llamadas
     * a setValue.
     * En el caso de las sentencias INSERT, sirve para agregar multiples registros a una misma sentencia.
     * Ej: ... VALUES (1, 2, 3), (A, B, C)
     * En caso de las sentencias UPDATE, solamente se podrá agregar 1 grupo de valores, ahorrándonos hacer multiples
     * llamadas a setValue.
     * Ej: ... SET a = 1 AND b = 2 AND c = 3.
     *
     * @param array $arrayValues
     *
     * @return $this
     * @throws \Exception
     */
    public function setValues(array $arrayValues)
    {
        $this->_valueType = 'multiple';
        $i = 1;
        $fieldsCount = 0;

        foreach($arrayValues as $k => $values) {

            foreach($values as $field => $value) {
                $this->_values[$k][] = self::_prepareParam($field, $value);
            }

            if ($i == 1) {
                $fieldsCount = count($this->_values[$k]);
            } else {
                if (count($this->_values[$k]) != $fieldsCount)
                    throw new \Exception(get_class($this) . ' | La cantidad de valores por registro no coinciden.');
            }

            $i++;

        }

        return $this;

    }

    /**
     * Agrega multiples valores para ser ejecutados en consultas independientes que deben ser ejecutados como Bash.
     * Este méto-do sustituye a la práctica de hacer un INSERT dentro de un bucle.
     *
     * Esté méto-do NO replaza a los méto-dos setValue y setValues, los que siempre deberán ser llamados para definir los
     * parámetros y los tipos de datos. Al momento de declarar los parámetros deberán ser definidos con valores nulos.
     * El méto-do setBashValue solamente deberá ser alimentado con los valores a ejecutar,
     * sin necesidad de definir nuevamente el nombre del parámetro y el tipo de datos, pues estos ya tendrían que haber
     * sido definidos con los méto-dos setValue o setValues.
     *
     * @example:
     * <code>
     * <?php
     * $sql->query('INSERT INTO t (v1, v2) VALUES ([v1], [v2])');
     * $sql->setValue('v1:int', null);
     * $sql->setValue('v2:int', null);
     * $sql->setBashValues(array(
     * array('v1' => 1, 'v2' => 1),
     * array('v1' => 2, 'v2' => 2)
     * ));
     * ?>
     * </code>
     *
     * @param array $arrayValues
     *
     * @return $this
     * @throws \Exception
     */
    public function setBashValues(array $arrayValues)
    {
        $this->getPrepare();

        // Definimos la bandera de ejecución en Bash como true.
        $this->_bashQuery = true;

        // Asignamos los valores.
        foreach ($arrayValues as $k => $values) {

            // Validamos que la cantidad de valores por ciclo sea igual a la cantidad de parámetros definidos.
            if (count($values) !== count($this->_params))
                throw new \Exception(get_class($this) . ' | La cantidad de valores por registro no coinciden.');

            // Obtenemos los parámetros.
            $prepareParams = $this->_params;

            foreach ($values as $field => $value) {
                // Preparamos los parametros
                $temp = self::_prepareParam($field, $value);
                if (!isset( $this->_params[ $temp['field'] ] ))
                    throw new \Exception(get_class($this) . ' | El parámetro '.$temp['field'] .' no fue definido en la declaración de la consulta.');

                $prepareParams[ $temp['field'] ] = $temp['value'];
            }

            $this->_bashParams[] = $prepareParams;

        }

        // Si la cantidad de valores a ser ejecutados en Bash es solo 1, no tiene sentido ejecutarlo en bach,
        // por lo que reasignamos los valores a los parametros normales para ser ejecutada como una consulta normal.
        if (count($this->_bashParams) == 1) {
            $this->_values = array();
            foreach ($this->_bashParams as $k => $values) {
                foreach ($values as $field => $value) {
                    // Asignamos los valores a los parámetros y a los valores.
                    $this->_params[ $field ]['value'] = $value;

                    $this->setValue($this->_params[ $field ]['field'].':'.$this->_params[ $field ]['type'] , $value);
                }
            }

            $this->_bashParams = array();
            $this->_bashQuery = false;
        }

        return $this;
    }

    /*
     * UPDATE
     */

    /**
     * Inicia un UPDATE.
     *
     * @param string $table
     * @return $this
     */
    public function update($table)
    {
        $this->clear();

        $this->_statement = 'update';
        $this->_setTable($table);

        return $this;
    }

    /*
     * DELETE
     */

    /**
     * Inicia un DELETE.
     *
     * @param string $table
     * @return $this
     */
    public function delete($table)
    {
        $this->clear();

        $this->_statement = 'delete';
        $this->_setTable($table);

        return $this;
    }

    /*
     * ATTRIBUTES
     */

    /**
     * Agrega un atributo a una consulta.
     *
     * Los atibutos son aquellos que van inmediatamente del Statement.
     * Por ejemplo: DISTINTC, IGNORE, etc.
     *
     * @param string $attribute
     * @return $this
     */
    public function setAttributes($attribute)
    {
        $this->_statement_attributes = $attribute;

        return $this;
    }

    /*
     * SQL
     */

    /**
     * Establece una consulta SQL a ser ejecutada.
     *
     * Sirve cuando no se desea crear el statement usando los meto-dos anteriores. En este caso
     * se pasa en el parametro $sql la consulta completa que se desea ejecutar. Los parametros
     * a remplazar deberan expresarse con corchetes [].
     *
     * @example SELECT * FROM tabla WHERE id = [id]
     *
     * @param string $sql
     * @param array $params
     * @return $this|bool
     * @throws \Exception
     */
    public function query($sql, array $params = array())
    {
        if (empty($sql))
            return false;

        $sql = preg_replace('/\r|\n/', ' ', $sql);
        $sql = preg_replace('/\s+/', ' ', $sql);
        $sql = trim($sql);

        $this->clear();
        $this->_statement = $sql;
        $this->_queryString = $sql;

        foreach ($params as $field => $value) {
            // Preparamos los parametros
            $temp = self::_prepareParam($field, $value);

            // Si el tipo es sql significa que se trata de un SubStatement
            if ($temp['type'] == 'sql') {

                // Eliminamos el substatement de los parametros para que no sea procesado.
                unset($params[$field]);

                // Anadimos el subsegmento a la consulta madre.
                $sql = str_replace('[' . $temp['field'] . ']', $this->_getSubStatementQuery($temp['value']), $sql);
                // Agregamos los parametros del subsegmento a los parametros de la consulta madre.
                $params = array_merge($params, $this->_getSubStatementParams($temp['value']));

                // Eliminamos el subsegmento
                unset($this->_subStatement[$temp['value']]);

                // Volvemos a ejecutar el meto-do query pero con la nueva consulta que incluye el subsegmento.
                return $this->query($sql, $params);
                break;

            }

            list($paramName, $param) = $this->_formatParam($temp['field']);
            $temp['param'] = $param;

            // Contamos la cantidad de veces que el marcador de posicion aparece dentro del queryString (solo deberia aparacer 1 ves)
            $finded = substr_count($this->_statement, $param);
            if ($finded == 0)
                // Si no aparece, omitimos el parámetro
                continue;
            elseif ($finded > 1)
                // Si aparece mas de una vez, lanzamos Exception
                throw new \Exception(get_class($this) . ' | Marcador de posición ' . $param . ' inválido. El marcador de posición ya existe.');

            // Validamos que cada parametro solo se incluya una vez dentro del array de parámetros
            if (!isset($this->_params[$paramName])) {

                if (is_array($temp['value'])) {

                    /*
                     * Si el valor del parametro es un array, creamos un parametro independiente por cada valor.
                     * Esto se usa en parametros como IN, donde lo normal es que hayan multiples valores.
                     * Ej. La consulta:
                     * SELECT * FROM tabla WHERE id IN ([ids]); donde el valor de [ids] es array(1, 2);
                     * Se convierte en:
                     * SELECT * FROM tabla WHERE id IN ([ids0], [ids1]); donde el valor de [ids0] = 1 y [ids1] = 2;
                     */

                    // Guardamos el nombre del parametro original
                    $singleParam = $param;
                    $arrayParam = array();

                    // Generamos un parametro por cada valor y asignamos un nuevo nombre al parametro
                    foreach ($temp['value'] as $k => $v) {
                        list($paramName, $param) = $this->_formatParam($temp['field'].$k);
                        $arrayParam[] = $param;
                        $this->_params[ $paramName ] = array(
                            'field' => $temp['field'].$k,
                            'type'  => $temp['type'],
                            'value' => $v,
                            'param' => $param
                        );
                    }

                    // Sobreescribimos el nombre del parametro original: [ids]; con los nombres de los nuevos: [ids0], [ids1].
                    $sql = str_replace($singleParam, implode(',', $arrayParam), $sql);
                    $this->_statement = $sql;
                    $this->_queryString = $sql;

                } else {
                    $this->_params[ $paramName ] = $temp;
                }
            } else
                throw new \Exception(get_class($this) . ' | Parámetro ' . $paramName . ' inválido. El parámetro ya existe.');
        }

        // Obtenemos los marcadores de posición dentro del queryString
        preg_match_all("/\[[\w]+\]/", $this->_statement, $paramPlaceholders, PREG_SET_ORDER);
        // ... y validamos si coinciden con la cantidad de parámetros pasados
        if (count($paramPlaceholders) != count($this->_params))
            throw new \Exception(get_class($this) . ' | La cantidad de marcadores de posición no coincide con la cantidad de parámetros pasados.');

        return $this;
    }

    /**
     * Almacena un subsegmento de consulta que luego sera usado por una consulta madre.
     *
     * @param string $placeholder
     * @param string $sql
     * @param array $params
     * @return $this
     */
    public function setSubStatement($placeholder, $sql, array $params = array())
    {
        $i = count($this->_subStatement, COUNT_RECURSIVE);

        // Renombramos el placeholder del campo para evitar duplicidad
        // en el nombre de los parametros
        $newParams = array();
        foreach($params as $field => $value) {
            $tmp = $this->_prepareParam($field, $value);

            $newParamName = $tmp['field'] . $i;

            $sql = str_replace('[' . $tmp['field'] . ']', '[' . $newParamName . ']', $sql);
            $newParams[$newParamName . ':' . $tmp['type']] = $value;
        }

        $this->_subStatement[$placeholder][] = array(
            'sql' => $sql,
            'params' => $newParams
        );

        $this->clear();

        return $this;
    }

    /**
     * Retorna un string con el subsegmento de consulta.
     *
     * @param string $placeholder
     * @return string
     * @throws \Exception
     */
    private function _getSubStatementQuery($placeholder)
    {
        if (!isset($this->_subStatement[$placeholder]))
            throw new \Exception(get_class($this) . ' | No se encontro el substatement.');

        $sql = array();
        foreach($this->_subStatement[$placeholder] as $subStatement) {
            $sql[] = $subStatement['sql'];
        }

        return implode(' ', $sql);
    }

    /**
     * Retorna los parametros de un subsegmento de consulta.
     *
     * @param string $placeholder
     * @return array
     * @throws \Exception
     */
    private function _getSubStatementParams($placeholder)
    {
        if (!isset($this->_subStatement[$placeholder]))
            throw new \Exception(get_class($this) . ' | No se encontro el substatement.');

        $params = array();
        foreach($this->_subStatement[$placeholder] as $subStatement) {
            foreach($subStatement['params'] as $paramName => $param) {
                $params[$paramName] = $param;
            }
        }

        return $params;
    }

    /*
     * PREPARE
     */

    /**
     * Escapa los valores pasados según el tipo de dato establecido.
     *
     * @param mixed $value
     * @param string|null $type
     * @return float|int|string
     */
    public static function prepareValue($value, $type = null)
    {
        // Retornamos valor boleano en caso necesario
        if ($type == 'bool') {
            return (empty($value)) ? 0 : 1;
        }

        // Retornamos nulo en caso necesario
        if ($value === '' || $value === null || $value === false) return 'NULL';

        // Retornamos segun tipo
        if ($type == 'string' || $type == 'date') {
            if (PHP_VERSION < 6) $value = get_magic_quotes_gpc() ? stripslashes($value) : $value;
            return "'" . $value . "'";
        } else if ($type == 'int') {
            return intval($value, 10);
        } else if ($type == 'real' || $type == 'float') {
            return floatval($value);
        } else {
            if (PHP_VERSION < 6) $value = get_magic_quotes_gpc() ? stripslashes($value) : $value;
            return $value;
        }
    }

    /**
     * Prepara el valor para luego ser escapado.
     *
     * @param string $field
     * @param string $value
     * @return array
     */
    private static function _prepareParam($field, $value)
    {
        if (strpos($field, ':') !== false)
            list($field, $type) = explode(':', $field);
        else
            $type = null;

        return array(
            'field' => $field,
            'type'  => $type,
            'value' => $value
        );
    }

    /**
     * Formatea los parametros según se vallan a usar por MySQL o PDO.
     *
     * Por defecto, usará el formato establecido por _paramFormat, sin embargo
     * es posible forzar el formato pasandolo por la variable $format
     *
     * @param string $paramName
     * @param int|null $format
     * @return array
     * @throws \Exception
     */
    private function _formatParam($paramName, $format = null)
    {
        if (isset($this->_params[$paramName]) and
            in_array($this->_statement, array('select','insert','update','delete')))
            $paramName.= count($this->_params)+1;

        if (is_null($format))
            $format = $this->_paramFormat;

        // Si existen puntos (.) en el nombre del parametro lo eliminamos
        if (strpos($paramName, '.') !== false) {
            if (in_array($this->_statement, array('select','insert','update','delete'))) {
                $paramName = str_replace('.', '', $paramName);
            } else {
                throw new \Exception(get_class($this) . ' | Parámetro ' . $paramName . ' incorrecto. Los nombres de los parametros no pueden contener signos como puntos, comas, etc.');
            }
        }

        switch($format) {
            case 1:
                $param = '[' . $paramName . ']';
                break;
            case 2:
                $param = ':' . $paramName;
                break;
            case 3:
                $param = '?';
                break;
            default:
                $param = '[' . $paramName . ']';
                break;
        }

        return array($paramName, $param);
    }

    /*
     * GENERATE QUERY STATEMENT
     */

    /**
     * Genera un query statement preparado.
     *
     * @param int $paramFormat
     * @return mixed|string
     * @throws \Exception
     */
    public function getPrepare($paramFormat = 1)
    {
        // Se determina el tipo de formato a aplicar a los parametros
        $this->_paramFormat = $paramFormat;

        // Se genera el query segun sea un SELECT, INSERT, UPDATE o DELETE
        switch($this->_statement) {
            case 'select':
                // Limpiamos el array de parametros para llenarlo nuevamente
                $this->_params = array();
                $query = $this->_getSelectStatement();
                break;
            case 'insert':
                // Limpiamos el array de parametros para llenarlo nuevamente
                $this->_params = array();
                $query = $this->_getInsertStatement();
                break;
            case 'update':
                // Limpiamos el array de parametros para llenarlo nuevamente
                $this->_params = array();
                $query = $this->_getUpdateStatement();
                break;
            case 'delete':
                // Limpiamos el array de parametros para llenarlo nuevamente
                $this->_params = array();
                $query = $this->_getDeleteStatement();
                break;
        }

        if (isset($query)) {

            /*
             * Si la variable $query esta definida, significa que el statement se generó por medio de los méto-dos.
             * En este caso guardamos $query en la propiedad _queryString y la retornamos para su uso.
             */
            return $this->_queryString = $query;

        } elseif (isset($this->_queryString) and !empty($this->_queryString)) {

            /*
             * Si la variable $query no esta definida, significa que el query fue pasado mediante el méto-do "query".
             * En este caso analizamos los parametros y aplicamos el formato definido.
             */

            // Tomamos la consulta pasada por el usuario
            $query = $this->_statement;

            // .. y le damos formato a sus parametros
            foreach ($this->_params as $k => $p) {
                list($paramName, $oldParam) = $this->_formatParam($p['field'], 1);
                list($paramName, $param) = $this->_formatParam($p['field']);

                $this->_params[$k]['param'] = $param;

                $query = str_replace($oldParam, $param, $query);
            }

            // En este caso mantenemos el _queryString segun la definicion del usuario
            return $this->_queryString = $query;
        } else
            throw new \Exception(get_class($this) . ' | Imposible generar la consulta solicitada.');
    }

    /*
     * PARAMS
     */

    /**
     * Retorna los parámetros de la consulta en un array.
     *
     * @return array
     */
    public function getParams()
    {
        return $this->_params;
    }

    /**
     * Retorna los valores de los parámetros de una consulta en un array. Solamente se usa cuando se trate de consultas
     * Bash.
     *
     * @return array
     */
    public function getBashParams()
    {
        return $this->_bashParams;
    }

    /**
     * Toma una consulta preparada y remplaza los parametros por sus valores escapándolos.
     *
     * @return string
     * @throws \Exception
     */
    public function getProceced()
    {
        if (empty($this->_queryString))
            throw new \Exception(get_class($this) . ' | Imposible generar la consulta solicitada, el query string esta vacio.');

        $sql = $this->_queryString;

        foreach ($this->_params as $param) {
            $sql = str_replace(
                $param['param'],
                self::prepareValue($param['value'], $param['type']),
                $sql
            );
        }

        return $sql;
    }

    /**
     * Genera un SELECT y lo retorna como un string.
     *
     * @return string
     */
    private function _getSelectStatement()
    {
        $query = 'SELECT';

        if ($this->_statement_attributes) {
            $query.= ' ' . trim(strtoupper($this->_statement_attributes));
        }

        $fields = implode(', ', $this->_fields);
        $query.= ' ' . $fields;

        $query.= ' FROM ' . $this->_table;

        if (($join = $this->_getJoinStatement()) !== false) {
            $query.= ' ' . $join;
        }

        if (($where = $this->_getWhereStatement($this->_where)) !== false) {
            if (stripos($where, 'AND') === 0) {
                $where = trim(substr($where, 3));
            } elseif (stripos($where, 'OR') === 0) {
                $where = trim(substr($where, 2));
            }

            $query.= ' WHERE ' . $where;
        }

        if ($this->_group) {
            $query.= ' GROUP BY ' . implode(', ', $this->_group);
        }

        if (($having = $this->_getWhereStatement($this->_having)) !== false) {
            if (stripos($having, 'AND') === 0) {
                $having = trim(substr($having, 3));
            } elseif (stripos($having, 'OR') === 0) {
                $having = trim(substr($having, 2));
            }
            $query.= ' HAVING ' . $having;
        }

        if ($this->_orderBy) {
            $temp = array();
            foreach($this->_orderBy as $order) {
                $temp[] = implode(' ', $order);
            }
            $query.= ' ORDER BY ' . implode(', ', $temp);
            unset($temp);
        }

        if ($this->_limit)
            $query.= ' LIMIT ' . $this->_limit;

        return $query;

    }

    /**
     * Genera un INSERT y lo retorna como string.
     *
     * @return string
     */
    private function _getInsertStatement()
    {
        $query = 'INSERT';

        if ($this->_statement_attributes) {
            $query.= ' ' . trim(strtoupper($this->_statement_attributes));
        }

        $query.= ' INTO ' . $this->_table;

        $valueStatement = array();

        // Obtenemos valores a insertar
        if ($this->_valueType == 'multiple') {
            foreach($this->_values as $values) {
                $valueStatement[] = $this->_getValueStatement($values);
            }

            // TODO No me gusta esta forma de acceder al array (con el índice). Intenté usar current pero por alguna razón no me funcionó. Revisar please.
            $fieldsStatement = $this->_values[0];

        } else {
            $valueStatement[] = $this->_getValueStatement($this->_values);

            $fieldsStatement = $this->_values;
        }

        foreach ($fieldsStatement as $k => $field) {
            $fieldsStatement[$k] = $field['field'];
        }

        $query.= ' ('.implode(', ', $fieldsStatement).') VALUES ' . implode(', ', $valueStatement);

        return $query;
    }

    /**
     * Genera un string con los valores concatenados.
     *
     * @param array $values
     * @return string
     */
    private function _getValueStatement($values)
    {
        $valueStatement = array();
        foreach($values as $value) {

            list($paramName, $param) = $this->_formatParam($value['field']);
            $value['param'] = $param;

            $this->_params[$paramName] = $value;

            $valueStatement[] = $param;

        }

        $query = '('.implode(', ', $valueStatement).')';

        return $query;
    }

    /**
     * Genera un UPDATE y lo retorna como un string.
     *
     * @return string
     * @throws \Exception
     */
    private function _getUpdateStatement()
    {
        $query = 'UPDATE ' . $this->_table . ' SET';

        if ($this->_valueType == 'multiple') {

            if (count($this->_values) == 1)
                $values = $this->_values[0];
            else
                throw new \Exception(get_class($this) . ' | Imposible generar la consulta solicitada. No es posible multiples grupos de valores en sentencias UPDATE.');
        } else {
            $values = $this->_values;
        }

        $temp = array();
        foreach($values as $value) {
            list($paramName, $param) = $this->_formatParam($value['field']);
            $value['param'] = $param;

            $this->_params[$paramName] = $value;

            unset($value['type'], $value['value']);

            $temp[] = implode(' = ', $value);
        }

        $query.= ' ' . implode(', ', $temp);

        if (($where = $this->_getWhereStatement($this->_where)) !== false) {
            $query.= ' WHERE ' . $where;
        }

        if ($this->_limit)
            $query.= ' LIMIT ' . $this->_limit;

        return $query;
    }

    /**
     * Genera un DELETE y lo retorna como un string.
     *
     * @return string
     */
    private function _getDeleteStatement()
    {
        $query = 'DELETE FROM ' . $this->_table;

        if (($where = $this->_getWhereStatement($this->_where)) !== false) {
            $query.= ' WHERE ' . $where;
        }

        if ($this->_limit)
            $query.= ' LIMIT ' . $this->_limit;

        return $query;
    }

    /**
     * Genera un string con los JOIN's concatenados.
     *
     * @return bool|string
     */
    private function _getJoinStatement()
    {
        $temp = array();

        foreach ($this->_join as $join) {
            if (stripos($join['table'], ' as ') === false and stripos($this->_table, ' as ') === false) {
                $temp[] = sprintf('%s %s ON %s.%s = %s.%s',
                    strtoupper($join['type']),
                    $join['table'],
                    $this->_table,
                    $join['field1'],
                    $join['table'],
                    $join['field2']
                );
            } else {
                $temp[] = sprintf('%s %s ON %s = %s',
                    strtoupper($join['type']),
                    $join['table'],
                    $join['field1'],
                    $join['field2']
                );
            }
        }

        if (!empty($temp))
            return implode(' ', $temp);
        else
            return false;
    }

    /**
     * Genera un string con los WHERE's concatendados.
     *
     * @param array $wheres
     * @param bool $subStatement Indica si $where es un substatement
     * @return bool|string
     */
    private function _getWhereStatement($wheres, $subStatement = false)
    {
        if (!$wheres)
            return false;

        $temp = array();
        $firstCondition = null;
        $i = 1;

        foreach ($wheres as $where) {
            if (isset($where['subStatement'])) {
                $temp[] = $this->_getWhereStatement($where['subStatement'], true);
            } else {

                $condition = strtoupper($where['condition']);
                $where['operator'] = trim(strtoupper($where['operator']));

                unset($where['id'], $where['parentId'], $where['condition']);

                if (in_array($where['operator'], array('IN', 'NOT IN')) and is_array($where['value'])) {

                    foreach ($where['value'] as $k => $v) {
                        list($paramName, $param) = $this->_formatParam($where['field'].$k);
                        $where['param'][] = $param;
                        $this->_params[$paramName] = array(
                            'field' => $where['field'],
                            'type'  => $where['type'],
                            'value' => $v,
                            'param' => $param
                        );
                    }

                    $where['param'] = '(' . implode(',', $where['param']) . ')';

                } elseif ($where['operator'] == 'IS') {

                    list($paramName, $param) = $this->_formatParam($where['field']);
                    $where['param'] = $param;
                    $this->_params[$paramName] = array(
                        'field' => $where['field'],
                        'type'  => null,
                        'value' => strtoupper($where['value']),
                        'param' => $param
                    );

                } elseif ($where['operator'] == 'BETWEEN' and is_array($where['value'])) {

                    foreach ($where['value'] as $k => $v) {
                        list($paramName, $param) = $this->_formatParam($where['field'].$k);
                        $where['param'][] = $param;
                        $this->_params[$paramName] = array(
                            'field' => $where['field'],
                            'type'  => $where['type'],
                            'value' => $v,
                            'param' => $param
                        );
                    }

                    $where['param'] = implode(' AND ', $where['param']);

                } else {

                    list($paramName, $param) = $this->_formatParam($where['field']);
                    $where['param'] = $param;
                    $this->_params[$paramName] = array(
                        'field' => $where['field'],
                        'type'  => $where['type'],
                        'value' => $where['value'],
                        'param' => $param
                    );
                }

                if ($i > 1) {
                    $temp[] = $condition . ' ' . $where['field'] . ' ' . $where['operator'] . ' ' . $where['param'];
                } else {
                    $firstCondition = $condition;
                    $temp[] = $where['field'] . ' ' . $where['operator'] . ' ' . $where['param'];
                }

                $i++;
            }
        }

        if ($subStatement) {
            if (is_null($firstCondition)) {
                if (($pos = strpos($temp[0], '(')) !== false) {
                    $firstCondition = trim (strstr($temp[0], '(', true));
                    $temp[0] = substr($temp[0], $pos);
                }
            }
            $query = $firstCondition . ' (' . implode(' ', $temp) . ')';
        } else {
            foreach ($temp as $k => $v)
                $temp[$k] = trim($v);

            $query = implode(' ', $temp);

        }

        return $query;
    }

    /*
     * SHOW QUERY STATEMENT
     */

    /**
     * Retorna una consulta preparada.
     *
     * @return string
     */
    public function __toString()
    {
        $this->getPrepare();

        return $this->_queryString;
    }

    /**
     * Muestra en pantalla un query preparado o procesado.
     *
     * @param bool
     */
    public function showQuery($proceced = true)
    {
        $this->getPrepare();

        $html = '<div style="margin: 1em 1em 1em 0em; padding: 5px; border: 2px solid #444; background: #EEE; font-family: Courier; font-size: 16px; font-weight: bold; line-height: 1.2em;">';
        if ($proceced)
            $html.= $this->getProceced();
        else
            $html.= $this->_queryString;
        $html.= '</div>';

        echo $html;
    }

    public function getQuery()
    {
        $this->getPrepare();

        return $this->getProceced();
    }

    /**
     * Muestra en pantalla el arreglo de parametros.
     */
    public function showParams()
    {
        echo "\r\n<div style=\"border: solid 2px #CCC; padding: 5px\">\r\n";
        echo "<pre>\r\n";
        var_export($this->getParams());
        echo "\r\n</pre>\r\n";
        echo "</div>\r\n";
    }
}