//
//  QuillTests.swift
//  QuillTests
//
//  Created by Cole, Eric on 8/20/15.
//  Copyright © 2015 Cole, Eric. All rights reserved.
//

import XCTest
@testable import Quill

class QuillTests: XCTestCase {
	var connection = Connection()
	let simpleData = NSData( base64EncodedString:"eric" , options:NSDataBase64DecodingOptions( rawValue:0 ) )
	
	override func setUp() {
		super.setUp()
		connection.open()
	}
	
	override func tearDown() {
		connection.close()
		super.tearDown()
	}
	
	func testOpenConnection() {
		XCTAssertTrue( connection.isOpen )
	}
	
	func testActiveConnection() {
		let table_name = "TEST_ACTIVE_CONNECTION"
		let statement = try! connection.prepareStatement( "create table \"\(table_name)\" ( columnA int, columnB text )" )
		XCTAssertTrue( connection.hasStatements )
		statement.invalidate()
		XCTAssertFalse( connection.hasStatements )
	}
	
	func testCreateTable() {
		let table_name = "TEST_CREATE_TABLE"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnA int, columnB text )" )
		
		XCTAssertTrue( create )
	}
	
	func testInsertRows() {
		let table_name = "TEST_INSERT_ROWS"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnA int, columnB text )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnA, columnB ) values ( 1, \"TEXT\" )" )
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
	}
	
	func testInsertRowsWithParameters() {
		let table_name = "TEST_INSERT_PARAMETERS"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", 1, "TEST", 1.0, true, simpleData )
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
	}
	
	func testUpdateRowsWithParameters() {
		let table_name = "TEST_UPDATE_PARAMETERS"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnA int, columnB text )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnA, columnB ) values ( ?, ? )", 1, "TEST" )
		let update = try! connection.update( "update \"\(table_name)\" set columnA = ?", 2 )
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
		XCTAssertEqual( 1, update )
	}
	
	func testPrepareThrowsError() {
		do {
			try connection.prepareStatement( "create table invalid statement" )
			
			XCTFail()
		} catch let error {
			print( "prepare error \(error)" )
		}
	}
	
	func testPrepareWithParametersThrowsError() {
		let table_name = "TEST_THROW_WITH_PARAMETERS"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		
		let valueInteger = 1
		let valueString = "TEST"
		let valueDouble = 1.0
		let valueBoolean = true
		let valueData = simpleData
		let valueNull = NSNull()
		
		XCTAssertTrue( create )
		
		do {
			try connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", valueInteger, valueString, valueDouble, valueBoolean, valueData, valueNull )
			XCTFail()
		} catch let error {
			print( "prepare error \(error)" )
		}
		
		do {
			try connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", valueInteger )
			XCTFail()
		} catch let error {
			print( "prepare error \(error)" )
		}
	}
	
	func testQueryRowsAsObjects() {
		let table_name = "TEST_QUERY_ROWS"
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", 1, "TEST", 1.0, true, simpleData )
		
		struct RowData { let columnLong:Int64, columnString:String?, columnDouble:Double, columnBoolean:Bool }
		
		let rows = try! connection.select( "select * from \"\(table_name)\"", transform: { row -> RowData in
			return RowData( columnLong:row.columnLong(0), columnString:row.columnString(1), columnDouble:row.columnDouble(2), columnBoolean:row.columnBoolean(3) )
		} )
		let count = rows.count
		let row0 = rows[0]
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
		XCTAssertEqual( 1, count )
		XCTAssertEqual( 1, row0.columnLong )
		XCTAssertEqual( "TEST", row0.columnString! )
	}
	
	func testQueryRowsAsArrays() {
		let table_name = "TEST_QUERY_COLUMN_ARRAYS"
		
		let valueInteger = 1
		let valueString = "TEST"
		let valueDouble = 1.0
		let valueBoolean = true
		let valueData = simpleData
		let valueNull = NSNull()
		
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", valueInteger, valueString, valueDouble, valueBoolean, valueData )
		let second = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger ) values ( ? )", valueNull )
		
		let result = try! connection.gather( "select * from \"\(table_name)\"" )
		let rows = result.columns[0].count ?? 0
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
		XCTAssertTrue( insert < second )
		XCTAssertEqual( ["columnInteger","columnString","columnDouble","columnBoolean","columnData"], result.names )
		XCTAssertEqual( 2, rows )
		
		XCTAssertEqual( valueInteger , (result.columns[0][0] as! Int) )
		XCTAssertEqual( valueString , (result.columns[1][0] as! String) )
		XCTAssertEqual( valueDouble , (result.columns[2][0] as! Double) )
		XCTAssertEqual( valueBoolean , (result.columns[3][0] as! Bool) )
		XCTAssertEqual( valueData! , (result.columns[4][0] as! NSData) )
		
		XCTAssertEqual( valueNull , (result.columns[0][1] as! NSNull) )
		XCTAssertEqual( valueNull , (result.columns[1][1] as! NSNull) )
		XCTAssertEqual( valueNull , (result.columns[2][1] as! NSNull) )
		XCTAssertEqual( valueNull , (result.columns[3][1] as! NSNull) )
		XCTAssertEqual( valueNull , (result.columns[4][1] as! NSNull) )
	}
	
	func testEnumerateRowsWithColumns() {
		let table_name = "TEST_QUERY_COLUMNS_OF_ROWS"
		
		let valueInteger = 1
		let valueString = "TEST"
		let valueDouble = 1.0
		let valueBoolean = true
		let valueData = simpleData
		let valueNull = NSNull()
		
		let create = try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		let insert = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", valueInteger, valueString, valueDouble, valueBoolean, valueData )
		let second = try! connection.insert( "insert into \"\(table_name)\" ( columnInteger ) values ( ? )", valueNull )
		
		let pool = NSMutableSet()
		let null = NSNull()
		let query = try! connection.prepareStatement( "select * from \"\(table_name)\"" )
		
		for row in query {
			for var index = 0 ; index < row.columnCount ; ++index {
				print( "row column \(index) value \(row.columnObject(index, null:null, pool:pool))" )
			}
		}
		
		XCTAssertTrue( create )
		XCTAssertEqual( 1, insert )
		XCTAssertTrue( insert < second )
	}
	
	func testTraceExecutionHooks() {
		let table_name = "TEST_TRACE"
		
		connection.profileExecution( true )
		connection.traceExecution( true )
		connection.traceHooks( true )
		
		try! connection.execute( "create table \"\(table_name)\" ( columnInteger int, columnString text, columnDouble real, columnBoolean boolean, columnData blob )" )
		try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", 5, "five", 0.5e1, true, simpleData )
		try! connection.insert( "insert into \"\(table_name)\" ( columnInteger, columnString, columnDouble, columnBoolean, columnData ) values ( ?, ?, ?, ?, ? )", 7, "seven", 0.7e1, false, nil )
		try! connection.update( "update \"\(table_name)\" set columnInteger = ?", 9 )
		try! connection.update( "delete from \"\(table_name)\" where columnString = ?", "five" )
		try! connection.gather( "select * from \"\(table_name)\"" )
		
		connection.profileExecution( false )
		connection.traceExecution( false )
		connection.traceHooks( false )
	}
	
	func testTransactions() {
		let hasTransactionBeforeBegin = connection.hasTransaction
		try! connection.beginTransaction()
		let hasTransactionBeforeCommit = connection.hasTransaction
		try! connection.commit()
		let hasTransactionAfterCommit = connection.hasTransaction
		
		XCTAssertFalse( hasTransactionBeforeBegin )
		XCTAssertTrue( hasTransactionBeforeCommit )
		XCTAssertFalse( hasTransactionAfterCommit )
	}
	
	func testAttachDatabase() {
		let database_name = "TEST_ATTACH"
		
		try! connection.attachDatabase( database_name )
		try! connection.detachDatabase( database_name )
	}
	
	func testCreateTableWithColumnDeclarations() {
		let table_name = "TEST_COLUMN_DECLARATIONS"
		let columns = [
			ColumnDeclaration.primaryKey( "identifier" ),
			ColumnDeclaration.text( "label", collate:Connection.escape("natural") ),
			ColumnDeclaration.integer( "count", defaults:1, unique:false ),
			ColumnDeclaration.real( "value" ),
			ColumnDeclaration.boolean( "flag" )
		]
		let columnDeclarations = ColumnDeclaration.join( columns )
		
		try! connection.createTable( table_name, columnDeclarations:columnDeclarations, temporary:false, strict:true, withRowIdentifiers:false )
		
		let other_name = "TEST_FOREIGN_KEY"
		let otherDeclarations = ColumnDeclaration.join( [ColumnDeclaration.foriegnKey( "identifier", foreignKey:table_name + "(identifier)" ),ColumnDeclaration.text( "element", collate:"'numeric'" )] )
		
		try! connection.createTable( other_name, columnDeclarations:otherDeclarations, temporary:false, strict:false, withRowIdentifiers:true )
	}
	
	func testNaturalCollationOrder() {
		let table_name = "TEST_COLLATION_ORDER"
		let insert = ["åbc123","ABC99","ABÇ77","abc88"]
		let expect = ["ABÇ77","abc88","ABC99","åbc123"]
		var actual = [String]()
		
		try! connection.execute( "create table " + table_name + " ( columnText text collate 'natural' not null default '' )" )
		let statement = try! connection.prepareStatement( "insert into " + table_name + " ( columnText ) values ( ? )" )
		for string in insert {
			statement.reset()
			try! statement.bind( [string], startingAt:-1 )
			try! statement.executeInsert()
		}
		try! connection.prepareStatement( "select * from " + table_name + " order by columnText" ).oneColumn( &actual )
		
		XCTAssertEqual( actual, expect )
	}
	
	func testBindableBoolean() {
		let boolean = true
		switch ( boolean.bindableValue ) {
		case .Boolean(let value): XCTAssertEqual( boolean , value )
		default: XCTFail()
		}
	}
	
	func testBindableInteger() {
		let integer = 3
		switch ( integer.bindableValue ) {
		case .Integer(let value): XCTAssertEqual( integer , value )
		default: XCTFail()
		}
	}
	
	func testBindableReal() {
		let real = 3.5
		switch ( real.bindableValue ) {
		case .Real(let value): XCTAssertEqual( real , value )
		default: XCTFail()
		}
	}
	
	func testBindableString() {
		let string = "†ést"
		switch ( string.bindableValue ) {
		case .UTF8(let utf8,let length):
			let value = String(UTF8String:UnsafePointer<Int8>(utf8))
			XCTAssertEqual( string, value! )
			XCTAssertEqual( length, value!.lengthOfBytesUsingEncoding(NSUTF8StringEncoding) )
		default: XCTFail()
		}
	}
	
	func testBindableNumber() {
		let real = NSNumber(double: 2.5)
		switch ( real.bindableValue ) {
		case .Real(let value): XCTAssertEqual( real.doubleValue , value )
		default: XCTFail()
		}
		
		let integer = NSNumber(long: 5)
		switch ( integer.bindableValue ) {
		case .Long(let value): XCTAssertEqual( integer.longLongValue , value )
		default: XCTFail()
		}
	}
	
	func testBindableNull() {
		let null = NSNull()
		switch ( null.bindableValue ) {
		case .Null: XCTAssertTrue( true )
		default: XCTFail()
		}
	}
	
	func testBindableData() {
		let data = simpleData
		switch ( data!.bindableValue ) {
		case .Blob(let blob, let length):
			let value = NSData( bytes:blob, length:length )
			XCTAssertEqual( data, value )
		default: XCTFail()
		}
	}
	
	func testBindableArray() {
		let bytes:[Int8] = [1,2,3,4,5,6]
		switch ( bytes.bindableValue ) {
		case .Blob(let blob, let length):
			let value = NSData( bytes:blob, length:length )
			XCTAssertEqual( NSData( bytes:bytes, length:bytes.count ), value )
		default: XCTFail()
		}
		
		let empty:[Int8] = []
		switch ( empty.bindableValue ) {
		case .Null: XCTAssertTrue( true )
		default: XCTFail()
		}
		
		let protocols:[BinaryRepresentable] = [1,2,3,4,5,6]
		switch ( protocols.bindableValue ) {
		case .Null: XCTAssertTrue( true )
		default: XCTFail()
		}
		
		let references:[String] = ["a","b","c"]
		switch ( references.bindableValue ) {
		case .Null: XCTAssertTrue( true )
		default: XCTFail()
		}
	}
	
	func toBindable<T>( b:UnsafePointer<T> ) -> Bindable {
		return b
	}
	
	func testBindablePointer() {
		var geometry = CGRect()
		switch ( toBindable( &geometry ).bindableValue ) {
		case .Blob(_, let length):
			XCTAssertEqual( sizeof(CGRect), length )
		default: XCTFail()
		}
	}
	
	func testBindableZero() {
		let zero = BindableZeroData(length: 32)
		switch ( zero.bindableValue ) {
		case .Zero(let length):
			XCTAssertEqual( zero.length, length )
		default: XCTFail()
		}
	}
	
	func testReadme() {
		try! connection.execute( "create table mytable ( column text )" )
		try! connection.insert( "insert into mytable ( column ) values ( 'words' )" )
		try! connection.update( "update mytable set column = 'words'" )
		let values:[Int] = try! connection.select( "select count(*) from mytable", transform: { $0.columnInteger(0) } )
		
		print( "readme \(values)" )
		
		let query = try! connection.select( "select * from mytable" )
		var array = [String]()
		for row in query {
			array.append( row[0]! )
		}
		
		try! connection.insert( "insert into mytable ( column ) values ( ? )", "anything" )
		try! connection.execute( "create table thetable ( anInt integer, aStr text, aReal real, aBool boolean, someData blob )" )
		try! connection.insert( "insert into thetable ( anInt, aStr, aReal, aBool, someData ) values ( ?, ?, ?, ?, ? )", 5, "five", 5.5, false, "fiddy" )
	}
}
