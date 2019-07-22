<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 25/4/2018/22:40
 * Description:
 */
include '../Sql.php';
include '../GestorDB.php';
include '../GestorMySQL.php';

$bd = \Librerias\Datos\GestorMySQL::obtenerInstancia('produccion', array(
    'servidor'      => 'localhost',
    'usuario'       => 'root',
    'contrasena'    => '',
    'basedatos'     => 'test',
    'persistente'   => true
));

$sql = new \Librerias\Datos\Sql();

$sql->insertInto('clientes')
    ->setValue('nombre:string', 'Juan')
    ->setValue('apellido:string', 'Perez')
    ->setValue('edad:int', 20)
    ->setValue('fecha_registro:date', date('Y-m-d'))
    ->showQuery();

//$bd->sql($sql)->ejecutar();

$sql->insertInto('clientes')
    ->setValues(
        array(
            array(
                'nombre:string' => 'Carlos',
                'apellido:string' => 'Lopez',
                'edad:int' => 21,
                'fecha_registro:date' => date('Y-m-d'),
                'activo:int' => 1
            ),
            array(
                'nombre:string' => 'Maria',
                'apellido:string' => 'Dominguez',
                'edad:int' => 25,
                'fecha_registro:date' => date('Y-m-d'),
                'activo:int' => 1
            )
        )
    )->showQuery();

//$bd->sql($sql)->ejecutar();

$sql->insertInto('clientes')
    ->setValue('nombre:string', null)
    ->setValue('apellido:string', null)
    ->setValue('edad:int', null)
    ->setValue('fecha_registro:date', null)
    ->setBashValues(
        array(
            array(
                'nombre' => 'Xiomara',
                'apellido' => 'Fernandez',
                'edad' => 15,
                'fecha_registro' => date('Y-m-d')
            ),
            array(
                'nombre' => 'Roberta',
                'apellido' => 'Meneses',
                'edad' => 36,
                'fecha_registro' => date('Y-m-d')
            )
        )
    )->showQuery();

//$bd->sql($sql)->ejecutar();

$sql->query('INSERT INTO clientes (nombre, apellido, edad, fecha_registro) 
             VALUES ([nombre], [apellido], [edad], [fecha_registro])', array(
    'nombre:string' => null,
    'apellido:string' => null,
    'edad:int' => null,
    'fecha_registro:date' => null))
    ->setBashValues(
        array(
            array(
                'nombre' => 'Adolfo',
                'apellido' => 'Ortega',
                'edad' => 51,
                'fecha_registro' => date('Y-m-d')
            ),
            array(
                'nombre' => 'Isidro',
                'apellido' => 'Arellano',
                'edad' => 36,
                'fecha_registro' => date('Y-m-d')
            )
        )
)->showQuery();

//$bd->sql($sql)->ejecutar();

