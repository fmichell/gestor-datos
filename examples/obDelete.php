<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 26/4/2018/10:45
 * Description:
 */
include '../Sql.php';

$sql = new \Librerias\Datos\Sql();

$sql->delete('clientes')
    ->initWhere()
    ->andW('edad:int', '>', 15)
    ->closeWhere()
    ->showQuery();