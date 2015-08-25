# Quill
Quill is a thin wrapper around sqlite3 written in swift.

## Connection
A Quill Connection is the wrapper around the `sqlite3` object.  A Connection is created by opening a connection to a database.  Other databases may be attached to or detached from a connection.

A Quill Connection may be used to prepare statements for execution.  It also has several convenience methods to prepare, bind, execute and gather results from a statement.

```
let connection = Connection()
try! connection.execute( "create table mytable ( column text )" )
try! connection.insert( "insert into mytable ( column ) values ( ? )", "words" )
try! connection.update( "update mytable set column = ?", "phrases" )
let values:[Int] = try! connection.select( "select count(*) from mytable", transform: { $0.columnIntegerValue(0) } )
```

The only difference between execute, insert and update is the return value.  Execute returns true or false, insert returns the identifier of the most recently inserted row, and update returns the number of rows updated.  If the return value will be ignored use execute.  Using execute, update or insert for a select statement will discard results.

A Connection uses a temporary, in memory database unless a file path or file uri is specified.

## Statement
A Quill Statement represents a compiled sql statement.  Statements may have variables bound to positional parameters.  Statements may be executed or, for select statements, iterated over to gather results.  A Statement is a SequenceType so, like an array, all rows can be visited with a `for in` loop.

```
let connection = Connection()
let query = try! connection.prepareStatement( "select * from mytable" )
var array = [NSString]()
for row in query {
		array.append( row.columnStringValue(0) )
}
```

There are also convenience methods to gather all results as arrays of AnyObject or as arrays of column specific types using column major order.

## Bindable
When binding values to statement parameters a Bindable represents the value to bind.  All common swift types like Int, String and Bool have Bindable extensions.  The Bindable protocol returns a BindableValue that provides the type and value to bind.

```
let connection = Connection()
try! connection.execute( "create table thetable ( anInt integer, aStr text, aReal real, aBool boolean, someData blob )" )
try! connection.insert( "insert into thetable ( anInt, aStr, aReal, aBool, someData ) values ( ?, ?, ?, ?, ? )", 5, "five", 5.5, false, "fiddy" )
```

## Primer
The Primer extension to Connection provides implementations of several simple statements.  The core of sql is insert, update and select statements, which are not covered by Primer.  Primer does help with table management, transactions, savepoints, and other auxiliary tasks.

## Collation
In addition to the standard BINARY and NOCASE collation methods, quill supports many named combinations of NSStringCompareOptions for collation.  For example, `'natural'` collation is insensitive to case, diacritics, and width and compares integers by value within strings.

## Throwing
Many Quill methods throw errors.  The two most common errors are statement compilation and binding the wrong number of arguments.  If statements and parameters are not generated at runtime then `try!` can be used to ignore errors.

## Threading
sqlite3 is sort of thread safe, and Quill does not enhance thread safety.  For best results, use a dispatch queue to serialize all access to a single Connection instance.  Otherwise, use a separate Connection instance for each thread.  Sharing a single Connection instance across multiple threads is not recommended.

## Shortcomings
Quill does not currently support virtual tables, custom functions or collations, hooks or other callbacks, iterating over columns within a row, or help with building complex statements.


