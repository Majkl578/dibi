<?php

/**
 * Compatibility with Nette
 */
if (!class_exists('NotImplementedException', FALSE)) {
	class NotImplementedException extends LogicException {}
}

if (!class_exists('NotSupportedException', FALSE)) {
	class NotSupportedException extends LogicException {}
}

if (!class_exists('MemberAccessException', FALSE)) {
	class MemberAccessException extends LogicException {}
}

if (!class_exists('InvalidStateException', FALSE)) {
	class InvalidStateException extends RuntimeException {}
}

if (!class_exists('IOException', FALSE)) {
	class IOException extends RuntimeException {}
}

if (!class_exists('FileNotFoundException', FALSE)) {
	class FileNotFoundException extends IOException {}
}

if (!interface_exists('Nette\IDebuggable', FALSE)) {
	require_once __DIR__ . '/IDebuggable.php';
}