<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 26/4/2018/00:04
 * Description:
 */
require __DIR__ . '/../vendor/autoload.php';

$bd = \fmichell\GestorDatos\GestorMySQL::obtenerInstancia('produccion', array(
    'servidor'      => 'localhost',
    'usuario'       => 'root',
    'contrasena'    => '',
    'basedatos'     => 'test',
    'persistente'   => true
));

$sql = new \fmichell\GestorDatos\Sql();

$sql->update('clientes')
    ->setValue('nombre:string', 'Juan Carlos')
    ->initWhere()
    ->andW('id', '=', 1)
    ->closeWhere()
    ->showQuery();

$bd->sql($sql)->ejecutar();

$sql->query('UPDATE clientes SET nombre = [nombre] WHERE id = [id]', array(
    'nombre:string' => 'Carlos Ernesto',
    'id:int' => 2
))->showQuery();

$bd->sql($sql)->ejecutar();

$sql->update('clientes')
    ->setValues(
        array(
            array(
                'nombre:string' => 'Maria Jose',
                'apellido:string' => 'Dominguez Gonzalez'
            )
        )
    )
    ->initWhere()
    ->andW('id', '=', 3)
    ->closeWhere();
//$sql->showQuery();

$bd->sql($sql)->ejecutar();

$sql->query('UPDATE clientes SET nombre = [nombre] WHERE id = [id]', array(
    'nombre:string' => null,
    'id:int' => null
))->setBashValues(
    array(
        array(
            'nombre' => 'Xiomara Lorena',
            'id' => 4
        ),
        array(
            'nombre' => 'Roberta Alejandra',
            'id' => 5
        )
    )
)->showQuery();

$bd->sql($sql)->ejecutar();