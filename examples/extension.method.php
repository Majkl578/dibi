<h1>dibi extension method example</h1>
<pre>
<?php

require_once 'Nette/Debug.php';
require_once '../dibi/dibi.php';

use Dibi\dibi;


dibi::connect(array(
	'driver'   => 'sqlite',
	'database' => 'sample.sdb',
));



// using the "prototype" to add custom method to class DibiResult
function Dibi_Result_prototype_fetchShuffle(\Dibi\Result $obj)
{
	$all = $obj->fetchAll();
	shuffle($all);
	return $all;
}


// fetch complete result set shuffled
$res = dibi::query('SELECT * FROM [customers]');
$all = $res->fetchShuffle();
Debug::dump($all);
