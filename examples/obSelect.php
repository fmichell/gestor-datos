<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 25/4/2018/21:46
 * Description:
 */
include '../Sql.php';

$sql = new \Librerias\Datos\Sql();

$sql->select('campo1', 'campo2')
    ->from('tabla')
    ->initWhere()
    ->andW('campo1:string', '=', 'valor1')
    ->andW('campo2:string', '<>', 'valor2')
    ->closeWhere()
    ->orderBy('campo1', '+')
    ->orderBy('campo2', '-')
    ->limit(10, 20)
    ->showQuery();

$sql->select('campo1', 'campo2')
    ->from('tabla')
    ->initWhere()
    ->andW('campo1:int', '>', 1)
    ->andW('campo2:int', 'is', null)
    ->closeWhere()
    ->showQuery();

$sql->select('campo1', 'campo2')
    ->from('tabla')
    ->initWhere()
    ->andW('campo1:string', 'in', array('v1', 'v2', 'v3'))
    ->andW('campo2:int', 'not in', array(1, 2, 3))
    ->closeWhere()
    ->showQuery();

$sql->select('campo1', 'campo2', 'campo3')
    ->from('tabla')
    ->initWhere()
        ->andW('campo1:string', '=', 'valor')
        ->initWhere()
            ->initWhere()
                ->andW('campo2:int', 'between', array(1, 10))
                ->orW('campo2:int', '>', 20)
            ->closeWhere()
            ->initWhere()
                ->andW('campo3:string', '<>', 'valor')
                ->orW('campo3:string', 'like', '%hola%')
            ->closeWhere()
        ->closeWhere()
    ->closeWhere()
    ->showQuery();

$sql->select('campo', 'count(*) AS cantidad')
    ->from('tabla')
    ->groupBy('campo')
    ->initHaving()
    ->andH('cantidad', '>', 10)
    ->closeHaving()
    ->showQuery();

$sql->select('campo')
    ->setAttributes('distinct')
    ->from('tabla')
    ->initWhere()
    ->andW('DATE_FORMAT(fecha, "%Y-%m-%d")', '>', 'NOW()')
    ->closeWhere()
    ->showQuery();

$sql->select('t1.campo1', 't2.campo2', 't3.campo3', 't4.campo4')
    ->from('tabla1 as t1')
    ->innerJoin('tabla2 as t2', 't1.id', 't2.id')
    ->leftJoin('tabla3 as t3', 't2.id', 't3.id')
    ->rightJoin('tabla4 as t4', 't3.id', 't4.id')
    ->showQuery();

$sql->select('t1.campo1', 't2.campo2')
    ->from('tabla1 as t1')
    ->join('tabla2 as t2', 't1.id', 't2.id', 'inner')
    ->showQuery();