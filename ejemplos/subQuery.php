<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 26/4/2018/10:47
 * Description:
 */
require __DIR__ . '/../vendor/autoload.php';

$sql = new \fmichell\GestorDatos\Sql();

/*
 * SELECT nombre
 * FROM clientes
 * WHERE id IN (SELECT id FROM clientes WHERE edad > 15);
 */
$sql->setSubStatement('subselect', 'SELECT id FROM clientes WHERE edad > [edad]', array(
    'edad:int' => 15
))->query(
    'SELECT nombre FROM clientes WHERE id IN ([subselect])', array(
        'subselect:sql' => 'subselect'
    )
)->showQuery();
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/*
 * SELECT nombre_completo
 * FROM (SELECT CONCAT_WS(" ", nombre, apellido) AS nombre_completo FROM clientes) AS nombres
 * WHERE nombre_completo = (SELECT CONCAT_WS(" ", nombre, apellido) FROM clientes WHERE edad = 20);
 */
$sql->setSubStatement('from', 'select CONCAT_WS(" ", nombre, apellido) as nombre_completo from clientes')
    ->setSubStatement('filtro', 'select CONCAT_WS(" ", nombre, apellido) from clientes where edad = 20')
    ->query('
    SELECT nombre_completo
    FROM ([from]) AS nombres
    WHERE nombre_completo = ([filtro])', array(
        'from:sql' => 'from',
        'filtro:sql' => 'filtro'
    ))
    ->showQuery();
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/*
 * UPDATE clientes
 * SET edad = CASE
 * WHEN id = 1 THEN edad = 30
 * WHEN id = 2 THEN edad = 31
 * WHEN id = 3 THEN edad = 35
 * END
 * WHERE id IN (SELECT id FROM clientes WHERE activo IS NOT NULL);
 */
$arrayDatos = array(
    1 => 30,
    2 => 31,
    3 => 35
);

foreach ($arrayDatos as $id => $edad) {
    $sql->setSubStatement('edad', 'WHEN id = [id] THEN edad = [edad]', array(
        'id:int'   => $id,
        'edad:int' => $edad
    ));
}
$sql->setSubStatement('filtro', 'SELECT id FROM clientes WHERE activo IS NOT NULL');
$sql->query('
    UPDATE clientes
    SET edad = CASE 
    [sqlcase]
    END
    WHERE id IN ([filtro])', array(
        'sqlcase:sql' => 'edad',
        'filtro:sql' => 'filtro'
    ))
    ->showQuery();
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++