<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 26/4/2018/10:45
 * Description:
 */
require __DIR__ . '/../vendor/autoload.php';

$sql = new \Vigoron\GestorDatos\Sql();

$sql->delete('clientes')
    ->initWhere()
    ->andW('edad:int', '>', 15)
    ->closeWhere()
    ->showQuery();