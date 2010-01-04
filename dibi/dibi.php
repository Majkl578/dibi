<?php

/**
 * dibi - tiny'n'smart database abstraction layer
 * ----------------------------------------------
 *
 * Copyright (c) 2005, 2010 David Grudl (http://davidgrudl.com)
 *
 * This source file is subject to the "dibi license" that is bundled
 * with this package in the file license.txt, and/or GPL license.
 *
 * For more information please see http://dibiphp.com
 *
 * @copyright  Copyright (c) 2005, 2010 David Grudl
 * @license    http://dibiphp.com/license  dibi license
 * @link       http://dibiphp.com
 * @package    dibi
 */

namespace Dibi;

/**
 * Check PHP configuration.
 */
if (version_compare(PHP_VERSION, '5.3', '<')) {
	throw new \Exception('This version of dibi uses namespaces and therefore needs PHP 5.3 or newer.');
}

@set_magic_quotes_runtime(FALSE); // intentionally @


//nette compatibility
require __DIR__ . '/Nette/compatibility.php';


/**
 * Back-compatibility
 */
class Variable extends \DateTime
{
	function __construct($val)
	{
		parent::__construct($val);
	}
}



// dibi libraries
require_once __DIR__ . '/libs/interfaces.php';
require_once __DIR__ . '/libs/Object.php';
require_once __DIR__ . '/libs/Exception.php';
require_once __DIR__ . '/libs/Connection.php';
require_once __DIR__ . '/libs/Result.php';
require_once __DIR__ . '/libs/ResultIterator.php';
require_once __DIR__ . '/libs/Row.php';
require_once __DIR__ . '/libs/Translator.php';
require_once __DIR__ . '/libs/DataSource.php';
require_once __DIR__ . '/libs/Fluent.php';
require_once __DIR__ . '/libs/DatabaseInfo.php';
require_once __DIR__ . '/libs/Profiler.php';





/**
 * Interface for database drivers.
 *
 * This class is static container class for creating DB objects and
 * store connections info.
 *
 * @copyright  Copyright (c) 2005, 2010 David Grudl
 * @package    Dibi
 */
class dibi
{
	/**#@+
	 * dibi data type
	 */
	const TEXT =       's'; // as 'string'
	const BINARY =     'bin';
	const BOOL =       'b';
	const INTEGER =    'i';
	const FLOAT =      'f';
	const DATE =       'd';
	const DATETIME =   't';
	const TIME =       't';
	const IDENTIFIER = 'n';
	/**#@-*/

	/**#@+
	 * @deprecated column types
	 */
	const FIELD_TEXT = self::TEXT;
	const FIELD_BINARY = self::BINARY;
	const FIELD_BOOL = self::BOOL;
	const FIELD_INTEGER = self::INTEGER;
	const FIELD_FLOAT = self::FLOAT;
	const FIELD_DATE = self::DATE;
	const FIELD_DATETIME = self::DATETIME;
	const FIELD_TIME = self::TIME;
	/**#@-*/

	/**#@+
	 * dibi version
	 */
	const VERSION = '1.3-dev';
	const REVISION = '$WCREV$ released on $WCDATE$';
	/**#@-*/

	/**#@+
	 * Configuration options
	 */
	const RESULT_WITH_TABLES = 'resultWithTables'; // for MySQL
	const ROW_CLASS = 'rowClass';
	const ASC = 'ASC', DESC = 'DESC';
	/**#@-*/

	/** @var Connection[]  Connection registry storage for Connection objects */
	private static $registry = array();

	/** @var Connection  Current connection */
	private static $connection;

	/** @var array  Substitutions for identifiers */
	public static $substs = array();

	/** @var callback  Substitution fallback */
	public static $substFallBack = array(__CLASS__, 'defaultSubstFallback');

	/** @var array  @see addHandler */
	private static $handlers = array();

	/** @var string  Last SQL command @see Dibi\dibi::query() */
	public static $sql;

	/** @var int  Elapsed time for last query */
	public static $elapsedTime;

	/** @var int  Elapsed time for all queries */
	public static $totalTime;

	/** @var int  Number or queries */
	public static $numOfQueries = 0;

	/** @var string  Default dibi driver */
	public static $defaultDriver = 'mysql';



	/**
	 * Static class - cannot be instantiated.
	 */
	final public function __construct()
	{
		throw new \LogicException("Cannot instantiate static class " . get_called_class());
	}



	/********************* connections handling ****************d*g**/



	/**
	 * Creates a new Connection object and connects it to specified database.
	 * @param  array|string|\ArrayObject connection parameters
	 * @param  string       connection name
	 * @return Connection
	 * @throws Exception
	 */
	public static function connect($config = array(), $name = 0)
	{
		return self::$connection = self::$registry[$name] = new Connection($config, $name);
	}



	/**
	 * Disconnects from database (doesn't destroy Dibi\Connection object).
	 * @return void
	 */
	public static function disconnect()
	{
		self::getConnection()->disconnect();
	}



	/**
	 * Returns TRUE when connection was established.
	 * @return bool
	 */
	public static function isConnected()
	{
		return (self::$connection !== NULL) && self::$connection->isConnected();
	}



	/**
	 * Retrieve active connection.
	 * @param  string   connection registy name
	 * @return Dibi\Connection
	 * @throws Dibi\Exception
	 */
	public static function getConnection($name = NULL)
	{
		if ($name === NULL) {
			if (self::$connection === NULL) {
				throw new Exception('Dibi is not connected to database.');
			}

			return self::$connection;
		}

		if (!isset(self::$registry[$name])) {
			throw new Exception("There is no connection named '$name'.");
		}

		return self::$registry[$name];
	}



	/**
	 * Change active connection.
	 * @param  string   connection registy name
	 * @return void
	 * @throws Exception
	 */
	public static function activate($name)
	{
		self::$connection = self::getConnection($name);
	}



	/**
	 * Retrieve active connection profiler.
	 * @return IProfiler
	 * @throws Exception
	 */
	public static function getProfiler()
	{
		return self::getConnection()->getProfiler();
	}



	/********************* monostate for active connection ****************d*g**/



	/**
	 * Generates and executes SQL query - Monostate for Connection::query().
	 * @param  array|mixed      one or more arguments
	 * @return Result|int   result set object (if any)
	 * @throws Exception
	 */
	public static function query($args)
	{
		$args = func_get_args();
		return self::getConnection()->query($args);
	}



	/**
	 * Executes the SQL query - Monostate for Connection::nativeQuery().
	 * @param  string           SQL statement.
	 * @return Result|int   result set object (if any)
	 */
	public static function nativeQuery($sql)
	{
		return self::getConnection()->nativeQuery($sql);
	}



	/**
	 * Generates and prints SQL query - Monostate for Connection::test().
	 * @param  array|mixed  one or more arguments
	 * @return bool
	 */
	public static function test($args)
	{
		$args = func_get_args();
		return self::getConnection()->test($args);
	}



	/**
	 * Generates and returns SQL query as DataSource - Monostate for Connection::dataSource().
	 * @param  array|mixed      one or more arguments
	 * @return DataSource
	 */
	public static function dataSource($args)
	{
		$args = func_get_args();
		return self::getConnection()->dataSource($args);
	}



	/**
	 * Executes SQL query and fetch result - Monostate for Connection::query() & fetch().
	 * @param  array|mixed    one or more arguments
	 * @return Row
	 * @throws Exception
	 */
	public static function fetch($args)
	{
		$args = func_get_args();
		return self::getConnection()->query($args)->fetch();
	}



	/**
	 * Executes SQL query and fetch results - Monostate for Connection::query() & fetchAll().
	 * @param  array|mixed    one or more arguments
	 * @return array of Row
	 * @throws Exception
	 */
	public static function fetchAll($args)
	{
		$args = func_get_args();
		return self::getConnection()->query($args)->fetchAll();
	}



	/**
	 * Executes SQL query and fetch first column - Monostate for Connection::query() & fetchSingle().
	 * @param  array|mixed    one or more arguments
	 * @return string
	 * @throws Exception
	 */
	public static function fetchSingle($args)
	{
		$args = func_get_args();
		return self::getConnection()->query($args)->fetchSingle();
	}



	/**
	 * Executes SQL query and fetch pairs - Monostate for Connection::query() & fetchPairs().
	 * @param  array|mixed    one or more arguments
	 * @return string
	 * @throws Exception
	 */
	public static function fetchPairs($args)
	{
		$args = func_get_args();
		return self::getConnection()->query($args)->fetchPairs();
	}



	/**
	 * Gets the number of affected rows.
	 * Monostate for Connection::getAffectedRows()
	 * @return int  number of rows
	 * @throws Exception
	 */
	public static function getAffectedRows()
	{
		return self::getConnection()->getAffectedRows();
	}



	/**
	 * Gets the number of affected rows. Alias for getAffectedRows().
	 * @return int  number of rows
	 * @throws Exception
	 */
	public static function affectedRows()
	{
		return self::getConnection()->getAffectedRows();
	}



	/**
	 * Retrieves the ID generated for an AUTO_INCREMENT column by the previous INSERT query.
	 * Monostate for Connection::getInsertId()
	 * @param  string     optional sequence name
	 * @return int
	 * @throws Exception
	 */
	public static function getInsertId($sequence=NULL)
	{
		return self::getConnection()->getInsertId($sequence);
	}



	/**
	 * Retrieves the ID generated for an AUTO_INCREMENT column. Alias for getInsertId().
	 * @param  string     optional sequence name
	 * @return int
	 * @throws Exception
	 */
	public static function insertId($sequence=NULL)
	{
		return self::getConnection()->getInsertId($sequence);
	}



	/**
	 * Begins a transaction - Monostate for Connection::begin().
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws Exception
	 */
	public static function begin($savepoint = NULL)
	{
		self::getConnection()->begin($savepoint);
	}



	/**
	 * Commits statements in a transaction - Monostate for Connection::commit($savepoint = NULL).
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws Exception
	 */
	public static function commit($savepoint = NULL)
	{
		self::getConnection()->commit($savepoint);
	}



	/**
	 * Rollback changes in a transaction - Monostate for Connection::rollback().
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws Exception
	 */
	public static function rollback($savepoint = NULL)
	{
		self::getConnection()->rollback($savepoint);
	}



	/**
	 * Gets a information about the current database - Monostate for Connection::getDatabaseInfo().
	 * @return DatabaseInfo
	 */
	public static function getDatabaseInfo()
	{
		return self::getConnection()->getDatabaseInfo();
	}



	/**
	 * Import SQL dump from file - extreme fast!
	 * @param  string  filename
	 * @return int  count of sql commands
	 */
	public static function loadFile($file)
	{
		return self::getConnection()->loadFile($file);
	}



	/**
	 * Replacement for majority of dibi::methods() in future.
	 */
	public static function __callStatic($name, $args)
	{
		//if ($name = 'select', 'update', ...') {
		//	return self::command()->$name($args);
		//}
		return call_user_func_array(array(self::getConnection(), $name), $args);
	}



	/********************* fluent SQL builders ****************d*g**/



	/**
	 * @return Fluent
	 */
	public static function command()
	{
		return self::getConnection()->command();
	}



	/**
	 * @param  string    column name
	 * @return Fluent
	 */
	public static function select($args)
	{
		$args = func_get_args();
		return call_user_func_array(array(self::getConnection(), 'select'), $args);
	}



	/**
	 * @param  string   table
	 * @param  array
	 * @return Fluent
	 */
	public static function update($table, $args)
	{
		return self::getConnection()->update($table, $args);
	}



	/**
	 * @param  string   table
	 * @param  array
	 * @return Fluent
	 */
	public static function insert($table, $args)
	{
		return self::getConnection()->insert($table, $args);
	}



	/**
	 * @param  string   table
	 * @return Fluent
	 */
	public static function delete($table)
	{
		return self::getConnection()->delete($table);
	}



	/********************* data types ****************d*g**/



	/**
	 * @deprecated
	 */
	public static function datetime($time = NULL)
	{
		return new \DateTime(is_numeric($time) ? date('Y-m-d H:i:s', $time) : $time);
	}



	/**
	 * @deprecated
	 */
	public static function date($date = NULL)
	{
		return new \DateTime(is_numeric($date) ? date('Y-m-d', $date) : $date);
	}



	/********************* substitutions ****************d*g**/



	/**
	 * Create a new substitution pair for indentifiers.
	 * @param  string from
	 * @param  string to
	 * @return void
	 */
	public static function addSubst($expr, $subst)
	{
		self::$substs[$expr] = $subst;
	}



	/**
	 * Remove substitution pair.
	 * @param  mixed from or TRUE
	 * @return void
	 */
	public static function removeSubst($expr)
	{
		if ($expr === TRUE) {
			self::$substs = array();
		} else {
			unset(self::$substs[':'.$expr.':']);
		}
	}



	/**
	 * Sets substitution fallback handler.
	 * @param  callback
	 * @return void
	 */
	public static function setSubstFallback($callback)
	{
		if (!is_callable($callback)) {
			$able = is_callable($callback, TRUE, $textual);
			throw new \InvalidArgumentException("Handler '$textual' is not " . ($able ? 'callable.' : 'valid PHP callback.'));
		}

		self::$substFallBack = $callback;
	}



	/**
	 * Default substitution fallback handler.
	 * @param  string
	 * @return mixed
	 */
	public static function defaultSubstFallback($expr)
	{
		throw new \InvalidStateException("Missing substitution for '$expr' expression.");
	}



	/********************* misc tools ****************d*g**/



	/**
	 * Prints out a syntax highlighted version of the SQL command or Result.
	 * @param  string|Result
	 * @param  bool  return output instead of printing it?
	 * @return string
	 */
	public static function dump($sql = NULL, $return = FALSE)
	{
		ob_start();
		if ($sql instanceof Result) {
			$sql->dump();

		} else {
			if ($sql === NULL) $sql = self::$sql;

			static $keywords1 = 'SELECT|UPDATE|INSERT(?:\s+INTO)?|REPLACE(?:\s+INTO)?|DELETE|FROM|WHERE|HAVING|GROUP\s+BY|ORDER\s+BY|LIMIT|SET|VALUES|LEFT\s+JOIN|INNER\s+JOIN|TRUNCATE';
			static $keywords2 = 'ALL|DISTINCT|DISTINCTROW|AS|USING|ON|AND|OR|IN|IS|NOT|NULL|LIKE|TRUE|FALSE';

			// insert new lines
			$sql = " $sql ";
			$sql = preg_replace("#(?<=[\\s,(])($keywords1)(?=[\\s,)])#i", "\n\$1", $sql);

			// reduce spaces
			$sql = preg_replace('#[ \t]{2,}#', " ", $sql);

			$sql = wordwrap($sql, 100);
			$sql = htmlSpecialChars($sql);
			$sql = preg_replace("#\n{2,}#", "\n", $sql);

			// syntax highlight
			$sql = preg_replace_callback("#(/\\*.+?\\*/)|(\\*\\*.+?\\*\\*)|(?<=[\\s,(])($keywords1)(?=[\\s,)])|(?<=[\\s,(=])($keywords2)(?=[\\s,)=])#is", array(__CLASS__, 'highlightCallback'), $sql);
			$sql = trim($sql);
			echo '<pre class="dump">', $sql, "</pre>\n";
		}

		if ($return) {
			return ob_get_clean();
		} else {
			ob_end_flush();
		}
	}



	private static function highlightCallback($matches)
	{
		if (!empty($matches[1])) // comment
			return '<em style="color:gray">' . $matches[1] . '</em>';

		if (!empty($matches[2])) // error
			return '<strong style="color:red">' . $matches[2] . '</strong>';

		if (!empty($matches[3])) // most important keywords
			return '<strong style="color:blue">' . $matches[3] . '</strong>';

		if (!empty($matches[4])) // other keywords
			return '<strong style="color:green">' . $matches[4] . '</strong>';
	}



	/**
	 * Returns brief descriptions.
	 * @return string
	 * @return array
	 */
	public static function getColophon($sender = NULL)
	{
		$arr = array(
			'Number of SQL queries: ' . dibi::$numOfQueries
			. (dibi::$totalTime === NULL ? '' : ', elapsed time: ' . sprintf('%0.3f', dibi::$totalTime * 1000) . ' ms'),
		);
		if ($sender === 'bluescreen') {
			$arr[] = 'dibi ' . dibi::VERSION . ' (revision ' . dibi::REVISION . ')';
		}
		return $arr;
	}

}
