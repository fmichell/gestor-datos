<?php
/**
 * Created by PhpStorm.
 * User: Federico @fmichell
 * Date/Time: 25/4/2018/21:32
 * Description:
 */
require __DIR__ . '/../vendor/autoload.php';

$sql = new \fmichell\GestorDatos\Sql();

// Consulta simple
$sql->query('
SELECT campo1, campo2, campo3 
FROM tabla 
WHERE campo1 = [campo1]', array(
    'campo1:string' => 'valor'
))->showQuery();

$sql->query('
INSERT INTO tabla (campo1, campo2, campo3)
VALUES ([campo1], [campo2], [campo3])', array(
    'campo1:string' => 'valor',
    'campo2:int' => 1,
    'campo3:date' => date('Y-m-d H:i:s')
))->showQuery();