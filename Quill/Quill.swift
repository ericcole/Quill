//
//  Quill.swift
//  Quill
//
//  Created by Cole, Eric on 8/20/15.
//  Copyright © 2015 Cole, Eric. All rights reserved.
//

import Foundation

//	MARK: BindableValue

public enum BindableValue {
	case Null
	case Boolean(Bool)
	case Integer(Int)
	case Long(Int64)
	case Real(Double)
	case Text(String)
	case Data(NSData)
	case UTF8(UnsafePointer<Void>,byteCount:Int)
	case UTF16(UnsafePointer<Void>,byteCount:Int)
	case Blob(UnsafePointer<Void>,byteCount:Int)
	case Zero(byteCount:Int32)
	case Preserve
}

//	MARK: Bindable

public protocol Bindable {
	var bindableValue:BindableValue { get }
}

//	MARK: QueryValue Type

public enum QueryValue : Int32 {
	case Long = 1			//	SQLITE_INTEGER	Int64
	case Real = 2			//	SQLITE_FLOAT	Double
	case Text = 3			//	SQLITE_TEXT		NSString
	case Data = 4			//	SQLITE_BLOB		NSData
	case Null = 5			//	SQLITE_NULL		AnyObject
	case Boolean = 8		//					Bool
}

//	MARK: QueryRow

public protocol BinaryRepresentable {}

public protocol QueryRow {
	/** number of columns in select results **/
	var columnCount:Int { get }
	/** names of columns in select results **/
	var columnNames:[String] { get }
	
	func columnType( column:Int ) -> QueryValue
	func columnIsNull( column:Int ) -> Bool
	func columnBoolean( column:Int ) -> Bool
	func columnInteger( column:Int ) -> Int
	func columnLong( column:Int ) -> Int64
	func columnDouble( column:Int ) -> Double
	func columnString( column:Int ) -> String?
	func columnData( column:Int ) -> NSData?
	func columnValue<T>( column:Int ) -> T?
	func columnObject( column:Int, null:AnyObject, pool:NSMutableSet? ) -> AnyObject
	func columnArray<T where T:BinaryRepresentable>( column:Int ) -> [T]
	
	subscript( column:Int ) -> Bool { get }
	subscript( column:Int ) -> Int64 { get }
	subscript( column:Int ) -> Double { get }
	subscript( column:Int ) -> String? { get }
	subscript( column:Int ) -> NSData? { get }
}

public protocol QueryAppendable {
	/** add value to appendable **/
	mutating func queryAppend<T>( value:T )
	mutating func removeAll( keepCapacity keepCapacity:Bool )
	var isEmpty:Bool { get }
	var prefersQueryValue:QueryValue? { get }
}

//	MARK: -

public struct Error : ErrorType, CustomStringConvertible {
	public let code:Int32
	public let message:String
	public let detail:String
	public var description:String { return "SQLITE \(code) - \(message)" + ( !detail.isEmpty ? "\n\n\"" + detail + "\"\n" : "" ) }
}

//	MARK: -

public class Connection {
	private var connection:COpaquePointer = nil	//	struct sqlite3 *
	
	//	MARK: Initialization
	
	public init() {}
	public init( location:String, immutable:Bool = false ) throws { try openThrows( location, immutable:immutable ) }
	public init( connection:COpaquePointer ) { self.connection = connection; prepareConnection() }
	deinit { if connection != nil { sqlite3_close( connection ) } }
	
	//	MARK: Statements
	
	public func prepareStatement( sql:String ) throws -> Statement { return try Statement( sql:sql, connection:connection ) }
	public func prepareStatement( sql:String, bind:[Bindable?] ) throws -> Statement { return try prepareStatement( sql ).with( bind ) }
	public func prepareStatement( sql:String, bind:[String:Bindable] ) throws -> Statement { return try prepareStatement( sql ).with( bind ) }
	
	//	MARK: Statement Convenience
	
	public func execute( sql:String, _ bind:Bindable?... ) throws -> Bool { return try prepareStatement( sql, bind:bind ).execute() == SQLITE_DONE }
	public func insert( sql:String, _ bind:Bindable?... ) throws -> Int64 { return try prepareStatement( sql, bind:bind ).executeInsert() }
	public func insert( sql:String, bind:[String:Bindable] ) throws -> Int64 { return try prepareStatement( sql, bind:bind ).executeInsert() }
	public func update( sql:String, _ bind:Bindable?... ) throws -> Int { return try prepareStatement( sql, bind:bind ).executeUpdate() }
	public func update( sql:String, bind:[String:Bindable] ) throws -> Int { return try prepareStatement( sql, bind:bind ).executeUpdate() }
	
	public func select( sql:String, _ bind:Bindable?... ) throws -> Statement { return try prepareStatement( sql ).with( bind ) }
	public func select<T>( sql:String, transform:(QueryRow) -> T, _ bind:Bindable?... ) throws -> [T] { return try prepareStatement( sql, bind:bind ).map( transform ) }
	public func filter<T>( sql:String, transform:(QueryRow) -> T?, _ bind:Bindable?... ) throws -> [T] { return try prepareStatement( sql, bind:bind ).filter( transform ) }
	public func gather( sql:String, _ bind:Bindable?... ) throws -> (columns:[[AnyObject]],names:[String]) { let s = try prepareStatement( sql, bind:bind ); return try (s.columnMajorResults(),s.columnNames) }
	public func gather( sql:String, inout columns:[QueryAppendable], _ bind:Bindable?... ) throws { try prepareStatement( sql, bind:bind ).columnMajorResults( &columns ) }
	
	//	MARK: Connection Information
	
	public var isOpen:Bool { return nil != connection }
	public var hasStatements:Bool { return nil != connection && nil != sqlite3_next_stmt( connection, nil ) }
	public var hasTransaction:Bool { return nil == connection ? false : sqlite3_get_autocommit( connection ) == 0 }
	public var rowInserted:Int64 { return nil == connection ? -1 : sqlite3_last_insert_rowid( connection ) }
	public var changesSinceExecute:Int32 { return nil == connection ? -1 : sqlite3_changes( connection ) }
	public var changesSinceOpen:Int32 { return nil == connection ? -1 : sqlite3_total_changes( connection ) }
	public var errorCode:Int32 { return sqlite3_extended_errcode( connection ) }
	public var errorMessage:String? { let utf8 = sqlite3_errmsg( connection ); return ( nil == utf8 ) ? nil : String( UTF8String:utf8 ) }
	public var filePath:String? { let utf8 = nil == connection ? nil : sqlite3_db_filename( connection, "main" ); return ( nil == utf8 ) ? nil : String( UTF8String:utf8 ) }
	
	public static func memoryUsed() -> Int64 { return sqlite3_memory_used() }
	public static func memoryPeak( reset:Bool = false ) -> Int64 { return sqlite3_memory_highwater( reset ? 1 : 0 ) }
	
	//	MARK: Connection Management
	
	public static func errorDescription( code:Int32, connection:COpaquePointer ) -> String? {
		var utf8 = ( nil != connection ) ? sqlite3_errmsg( connection ) : nil
		if nil == utf8 { utf8 = sqlite3_errstr( code ) }
		return nil != utf8 ? String( UTF8String:utf8 ) : nil
	}
	
	/**
		open connection to sqlite database
		
		location: file path, file uri or empty string for in memory
		immutable: open database as read only and do not create
		returns: error code and description or (SQLITE_OK,nil) for no error
	*/
	public func open( location:String = "", immutable:Bool = false ) -> (code:Int32,String?) {
		close()
		
		var flags:Int32 = ( !immutable || location.isEmpty ) ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY
		
		if location.isEmpty { flags |= SQLITE_OPEN_MEMORY }
		else if nil != location.rangeOfString( ":" ) { flags |= SQLITE_OPEN_URI }
		
		//	database connections are not thread safe and should only be accessed from their original thread
		//	flags |= SQLITE_OPEN_NOMUTEX
		
		let status = sqlite3_open_v2( location, &connection, flags, nil )
		var string:String? = nil
		
		if ( nil == connection || !( SQLITE_OK == status ) ) {
			string = Connection.errorDescription( status, connection:connection ) ?? "open failed"
			
			if nil != connection && !( SQLITE_NOTICE == status || SQLITE_WARNING == status ) {
				sqlite3_close_v2( connection )
				connection = nil
			}
		}
		
		if nil != connection {
			prepareConnection()
		}
		
		return (status,string)
	}
	
	public func openThrows( location:String , immutable:Bool ) throws -> (code:Int32,String?) {
		let (code,string) = open( location, immutable:immutable )
		
		if !( SQLITE_OK == code || SQLITE_NOTICE == code || SQLITE_WARNING == code ) {
			if let description = string {
				throw Error( code:code, message:description, detail:location )
			}
		}
		
		return (code,string)
	}
	
	public func close() -> Int32 {
		var result = SQLITE_OK
		
		if connection != nil {
			result = sqlite3_close_v2( connection )
			connection = nil
		}
		
		return result
	}
	
	private func prepareConnection() {
		prepareCollation()
	}
	
	//	MARK: Connection Options
	
	public func abort() { sqlite3_interrupt( connection ) }
	public func releaseMemoryCache() { sqlite3_db_release_memory( connection ) }
	public func setTimeToSleepWhileBusy( milliseconds:Int32 ) { sqlite3_busy_timeout( connection, milliseconds ) }
	public func accessLimit( what:Int32, value:Int32 = -1 ) -> Int32 { return sqlite3_limit( connection, what, value ) }
	
	public var maximumStatement:Int32 {
		get { return accessLimit( SQLITE_LIMIT_SQL_LENGTH ) }
		set { accessLimit( SQLITE_LIMIT_SQL_LENGTH, value:newValue ) }
	}
	
	//	MARK: Connection Delegate
	
	///	Enclose collation names in single quotes
	public struct Collation {
		public static let CaseDiacriticWidthInsensitive = "equivalent"
		public static let DiacriticWidthInsensitive = "case"
		public static let CaseWidthInsensitive = "mark"
		public static let CaseDiacriticInsensitive = "width"
		public static let WidthInsensitive = "glyph"
		public static let DiacriticInsensitive = "base"
		public static let CaseInsensitive = "alphabet"
		public static let CaseDiacriticWidthSensitive = "strict"
		public static let Natural = "natural"
		public static let Literal = "literal"
		public static let Numeric = "numeric"
		public static let Backword = "backword"
		public static let Ordered = "ordered"
	}

	static func collationCompareOptions( name:String ) -> UInt {
		var result:UInt = 0
		
		if nil != name.rangeOfString(Collation.Literal) { result |= NSStringCompareOptions.LiteralSearch.rawValue }
		else if nil != name.rangeOfString(Collation.CaseDiacriticWidthInsensitive) || nil != name.rangeOfString(Collation.Natural) { result |= NSStringCompareOptions.CaseInsensitiveSearch.rawValue | NSStringCompareOptions.DiacriticInsensitiveSearch.rawValue | NSStringCompareOptions.WidthInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.DiacriticWidthInsensitive) { NSStringCompareOptions.DiacriticInsensitiveSearch.rawValue | NSStringCompareOptions.WidthInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.CaseWidthInsensitive) { result |= NSStringCompareOptions.CaseInsensitiveSearch.rawValue | NSStringCompareOptions.WidthInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.CaseDiacriticInsensitive) { result |= NSStringCompareOptions.CaseInsensitiveSearch.rawValue | NSStringCompareOptions.DiacriticInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.CaseInsensitive) { result |= NSStringCompareOptions.CaseInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.DiacriticInsensitive) { result |= NSStringCompareOptions.DiacriticInsensitiveSearch.rawValue }
		else if nil != name.rangeOfString(Collation.WidthInsensitive) { result |= NSStringCompareOptions.WidthInsensitiveSearch.rawValue }
		else if nil == name.rangeOfString(Collation.CaseDiacriticWidthSensitive) { result |= NSStringCompareOptions.CaseInsensitiveSearch.rawValue | NSStringCompareOptions.DiacriticInsensitiveSearch.rawValue }
		
		if nil != name.rangeOfString(Collation.Numeric) || nil != name.rangeOfString(Collation.Natural) { result |= NSStringCompareOptions.NumericSearch.rawValue }
		if nil != name.rangeOfString(Collation.Backword) { result |= NSStringCompareOptions.BackwardsSearch.rawValue }
		if nil != name.rangeOfString(Collation.Ordered) { result |= NSStringCompareOptions.ForcedOrderingSearch.rawValue }
		
		return result
	}

	func prepareCollation() {
		sqlite3_collation_needed( connection, nil ) { (_, connection, representation, name_utf8) -> Void in
			let name = String( UTF8String:name_utf8 )?.lowercaseString ?? ""
			let options = Connection.collationCompareOptions( name )
			let context = UnsafeMutablePointer<UInt>.alloc(2)
			let encoding:UInt
			switch representation {
			case SQLITE_UTF8: encoding = NSUTF8StringEncoding
			case SQLITE_UTF16BE: encoding = NSUTF16BigEndianStringEncoding
			case SQLITE_UTF16LE: encoding = NSUTF16LittleEndianStringEncoding
			default: encoding = NSUTF16StringEncoding
			}
			
			context[0] = encoding
			context[1] = options
			
			sqlite3_create_collation_v2(connection, name_utf8, representation, context, { (pointer, a_length, a_bytes, b_length, b_bytes) -> Int32 in
				let context:UnsafeMutablePointer<UInt> = UnsafeMutablePointer(pointer)
				let encoding = context[0]
				let options = NSStringCompareOptions( rawValue:context[1] )
				let a = NSString( bytesNoCopy:UnsafeMutablePointer(a_bytes), length:Int(a_length), encoding:encoding, freeWhenDone:false )
				let b = String( bytesNoCopy:UnsafeMutablePointer(b_bytes), length:Int(b_length), encoding:encoding, freeWhenDone:false )
				
				return ( nil != a ) ? ( nil != b ) ? Int32( a!.compare( b!, options:options ).rawValue ) : 1 : 0
			},{ pointer in
				pointer.dealloc(2)
			})
		}
	}
	
	func traceExecution( trace:Bool = true ) {
		sqlite3_trace( connection, trace ? {_,utf8 in print( "SQLITE-TRACE " + String( UTF8String:utf8 )! )} : nil, nil )
	}
	
	func profileExecution( profile:Bool = true ) {
		sqlite3_profile( connection, profile ? {_,utf8,nanoseconds in print( "SQLITE-PROFILE " + String( format:"%.3f seconds ", Double(nanoseconds)/1000000000.0 ) + String( UTF8String:utf8 )! ) } : nil, nil )
	}
	
	func traceHooks( trace:Bool = true ) {
		sqlite3_commit_hook( connection, trace ? { _ -> Int32 in print( "SQLITE-COMMIT" ); return 0 } : nil, nil )
		sqlite3_rollback_hook( connection, trace ? { _ in print( "SQLITE-ROLLBACK" ) } : nil, nil )
		sqlite3_update_hook( connection, trace ? { _, what, database_utf8, table_utf8, row in
			let label = ( SQLITE_DELETE == what ? "DELETE" : SQLITE_INSERT == what ? "INSERT" : SQLITE_UPDATE == what ? "UPDATE" : "HOOK-\(what)" )
			let table = ( String( UTF8String:database_utf8 ) ?? "" ) + "." + ( String( UTF8String:table_utf8 ) ?? "" )
			print( "SQLITE-" + label + " " + table + " row \(row)" )
		} : nil, nil )
		sqlite3_wal_hook( connection, trace ? { (_, connection, database_utf8, pages) -> Int32 in print( "SQLITE-WAL " + ( String( UTF8String:database_utf8 ) ?? "" ) + " pages \(pages)" ); return 0 } : nil, nil )
	}
	
	//	sqlite3_busy_handler
	//	sqlite3_set_authorizer
	//	sqlite3_progress_handler
	//	sqlite3_create_function
	//	sqlite3_commit_hook
	//	sqlite3_rollback_hook
	//	sqlite3_update_hook
	//	sqlite3_wal_hook
}

//	MARK: -

public class Statement {
	private var statement:COpaquePointer = nil
	
	private(set) public var message:String? = nil
	private(set) public var status:Int32 = 0
	private(set) public var row:Int = -1
	
	public init( sql:String, connection:COpaquePointer ) throws { try prepareThrows( sql, connection:connection ) }
	public init( statement:COpaquePointer ) { self.statement = statement }
	deinit { if nil != statement { sqlite3_finalize( statement ) } }
	
	//	MARK: Statement Control
	
	public func reset() { rewind(); unbind() }
	public func rewind() { if nil != statement { status = sqlite3_reset( statement ); row = -1 } }
	public func invalidate() { if nil != statement { status = sqlite3_finalize( statement ); statement = nil } }
	public func advance() throws -> Bool { return try step() == SQLITE_ROW }
	
	public func step() throws -> Int32 {
		let result = sqlite3_step( statement )
		
		status = result
		
		if SQLITE_ROW == result || SQLITE_DONE == result {
			row += 1
		} else {
			message = nil == statement ? "invalid statement" : Connection.errorDescription( status, connection:sqlite3_db_handle( statement ) ) ?? "execute error"
			
			if !( SQLITE_NOTICE == result || SQLITE_WARNING == result ) {
				throw Error( code:result, message:message!, detail:( sql ?? "" ) )
			}
		}
		
		return result
	}
	
	public func execute() throws -> Int32 { return ( row < 0 ? try step() : SQLITE_MISUSE ) }
	public func executeInsert() throws -> Int64 { return ( row < 0 ? try step() == SQLITE_DONE : false ) ? inserted : -1 }
	public func executeUpdate() throws -> Int { return ( row < 0 ? try step() == SQLITE_DONE : false ) ? changes : -1 }
	
	//	MARK: Statement Information
	
	public var hasRow:Bool { return SQLITE_ROW == status && sqlite3_data_count( statement ) > 0 }
	public var canAdvance:Bool { return SQLITE_ROW == status || ( row < 0 && nil != statement ) }
	public var readonly:Bool { return sqlite3_stmt_readonly( statement ) != 0 }
	public var isBusy:Bool { return sqlite3_stmt_busy( statement ) != 0 }
	public var sql:String? { let utf8 = sqlite3_sql( statement ); return ( nil == utf8 ) ? nil : String( UTF8String:utf8 ) }
	public var inserted:Int64 { return sqlite3_last_insert_rowid( sqlite3_db_handle( statement ) ) }
	public var changes:Int { return Int(sqlite3_changes( sqlite3_db_handle( statement ) )) }
	
	//	MARK: Bind Variables
	
	public func unbind() -> Int32 { return sqlite3_clear_bindings( statement ) }
	
	//	bind parameters may be ordered from 1 to bindableCount
	public var bindableCount:Int32 { return sqlite3_bind_parameter_count( statement ) }
	
	public func require( values:Bindable?... ) throws -> Statement { return try with( values ) }
	public func with( values:[Bindable?] ) throws -> Statement { try bind( values, startingAt:-1 ); return self }
	public func with( values:[String:Bindable] ) throws -> Statement { try bind( values, throwing:.MissingParameters ); return self }
	
	public func bind( order:Int32, value bindable:Bindable ) -> (code:Int32,String?) {
		var result = SQLITE_OK
		
		switch bindable.bindableValue {
		case .Preserve: result = SQLITE_OK	//	preserve previously bound value for order
		case .Null: result = sqlite3_bind_null( statement, order )
		case .Boolean(let b): result = sqlite3_bind_int( statement, order, b ? 1 : 0 )
		case .Integer(let i): result = sqlite3_bind_int64( statement, order, sqlite3_int64(i) )
		case .Long(let l): result = sqlite3_bind_int64( statement, order, l )
		case .Real(let r): result = sqlite3_bind_double( statement, order, r )
		case .Text(let s):
			let text = s.bindableText
			result = ( nil == text.utf8 || text.byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_text( statement, order, UnsafePointer<Int8>(text.utf8), Int32(text.byteCount), SQLITE_TRANSIENT )
		case .UTF8(let utf8, let byteCount):
			result = ( nil == utf8 || byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_text( statement, order, UnsafePointer<Int8>(utf8), Int32(byteCount), SQLITE_TRANSIENT )
		case .UTF16(let utf16, let byteCount):
			result = ( nil == utf16 || byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_text16( statement, order, utf16, Int32(byteCount), SQLITE_TRANSIENT )
		case .Data(let data):
			let blob = data.bindableBlob
			result = ( nil == blob.data || blob.byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_blob( statement, order, blob.data, Int32(blob.byteCount), SQLITE_TRANSIENT )
		case .Blob(let data, let byteCount):
			result = ( nil == data || byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_blob( statement, order, data, Int32(byteCount), SQLITE_TRANSIENT )
		case .Zero(let byteCount):
			result = ( byteCount < 0 )
				? sqlite3_bind_null( statement, order )
				: sqlite3_bind_zeroblob( statement, order, byteCount )
		}
		
		if SQLITE_OK != result {
			status = result
			message = Connection.errorDescription( result, connection:sqlite3_db_handle( statement ) )
			return (status,message)
		}
		
		return (result,nil)
	}
	
	public func bind( name:String, value bindable:Bindable ) -> (code:Int32,String?) {
		let order = sqlite3_bind_parameter_index( statement, name )
		
		return order > 0 ? bind( order, value:bindable ) : (SQLITE_MISMATCH,"no parameter named '\(name)'")
	}
	
	public enum BindingThrows { case No, Errors, MissingParameters }
	
	/**
		bind named parameters
	
		values: parameter values by name ("name",":name","$name","@name") or number ("1","?1")
		throwing: when to throw, defaults to not throwing
	
		returns: number of bound parameters
	 */
	public func bind( values:[String:Bindable], throwing:BindingThrows = .No ) throws -> Int {
		var result = 0
		let limit = bindableCount
		var order:Int32
		let null = NSNull()
		
		for ( order = 1 ; order <= limit ; ++order ) {
			let utf8 = sqlite3_bind_parameter_name( statement, order )
			let anonymous = nil == utf8 || 0 == utf8.memory
			let name = ( anonymous ? nil : String( UTF8String:utf8 ) ) ?? String( order )
			var bindable = values[name]
			
			if anonymous && nil == bindable {
				bindable = values[name.lowercaseString]
				
				if nil == bindable {
					let key = name.substringFromIndex( name.startIndex.successor() )
					bindable = values[key]
					
					if nil == bindable { bindable = values[key.lowercaseString] }
					if nil == bindable { bindable = values[key.uppercaseString] }
				}
			}
			
			if nil == bindable {
				if throwing == .MissingParameters { throw Error( code:SQLITE_MISMATCH, message:"missing parameter '\(name)'", detail:( sql ?? "" ) ) }
			} else {
				result += 1
			}
			
			let (code,string) = bind( order , value:bindable ?? null )
			
			if !( SQLITE_OK == code || SQLITE_NOTICE == code || SQLITE_WARNING == code ) {
				if throwing != .No { throw Error( code:code, message:( string ?? "bad parameter for '\(name)'" ), detail:( sql ?? "" ) ) }
			}
		}
		
		return result
	}
	
	/**
		bind ordered parameters
	
		values: values to bind in statement order ignoring names
		startingAt: first parameter position to bind starting at 1
	
		returns: zero if all parameters bound otherwise next parameter to bind
		throws: nothing unless startingAt is negative
	 */
	public func bind( values:[Bindable?], startingAt:Int32 = 1 ) throws -> Int32 {
		let limit = bindableCount
		var order:Int32 = max(abs( startingAt ),1)
		let require = startingAt < 0
		let null = NSNull()
		
		if require && Int32(values.count) + order != limit + 1 {
			throw Error( code:SQLITE_RANGE, message:"expecting \(limit + 1 - order) parameters", detail:( sql ?? "" ) )
		}
		
		for value in values {
			if ( order > limit ) { return -1 }
			
			let (code,string) = bind( order , value:( value ?? null ) )
			
			if !( SQLITE_OK == code || SQLITE_NOTICE == code || SQLITE_WARNING == code ) {
				if require { throw Error( code:code, message:( string ?? "bad parameter #'\(order)'" ), detail:( sql ?? "" ) ) }
				return order
			}
			
			order += 1
		}
		
		return order > limit ? 0 : order
	}
	
	//	MARK: Column Information
	
	/// columns are ordered starting from zero and less than columnCount
	public var columnCount:Int { return Int(sqlite3_column_count( statement )) }
	
	/// only meaningful after advance and before value retrieved from column
	public func columnType( column:Int ) -> QueryValue {
		var result = sqlite3_column_type( statement, Int32(column) )
		
		if ( SQLITE_INTEGER == result ) {
			let declared = sqlite3_column_decltype( statement, Int32(column) )
			
			if nil != declared && 0 == sqlite3_strnicmp( "bool", declared, 4 ) {
				result = SQLITE_BOOL
			}
		}
		
		return QueryValue( rawValue:result )!
	}
	
	public func columnDeclared( column:Int32 ) -> String? {
		let utf8 = sqlite3_column_decltype( statement, column )
		
		return ( nil == utf8 ) ? nil : String( UTF8String:utf8 )
	}
	
	public func columnName( column:Int ) -> String? {
		let names = columnNames
		
		return column < 0 || column >= names.count ? nil : names[column]
	}
	
	private var columnNameArray:[String]? = nil
	public var columnNames:[String] {
		if nil == columnNameArray {
			var names = [String]()
			let count = columnCount
			for var index = 0 ; index < count ; ++index {
				let utf8 = sqlite3_column_name( statement, Int32(index) )
				//if nil == utf8 { utf8 = sqlite3_column_origin_name( statement, Int32(column) ) }
				let name = ( nil == utf8 ) ? nil : String( UTF8String:utf8 )
				
				names.append( name ?? "\(index + 1)" )
			}
			
			columnNameArray = names
		}
		
		return columnNameArray!
	}
	
	//	MARK: Column Values
	
	/// only meaningful after advance and before value retrieved from column
	public func columnIsNull( column:Int ) -> Bool {
		return ( SQLITE_NULL == sqlite3_column_type( statement, Int32(column) ) )
	}
	
	/// only meaningful after value retrieved from column and before advance
	public func columnWasNull( column:Int ) -> Bool {
		return nil == sqlite3_column_text( statement, Int32(column) )
	}
	
	public func columnBoolean( column:Int ) -> Bool {
		return sqlite3_column_int( statement, Int32(column) ) != 0
	}
	
	public func columnInteger( column:Int ) -> Int {
		return Int(sqlite3_column_int64( statement, Int32(column) ))
	}
	
	public func columnLong( column:Int ) -> Int64 {
		return sqlite3_column_int64( statement, Int32(column) )
	}
	
	public func columnDouble( column:Int ) -> Double {
		return sqlite3_column_double( statement, Int32(column) )
	}
	
	public func nullableBoolean( column:Int ) -> Bool? {
		return columnIsNull( column ) ? nil : columnBoolean( column )
	}
	
	public func nullableLong( column:Int ) -> Int64? {
		return columnIsNull( column ) ? nil : columnLong( column )
	}
	
	public func nullableDouble( column:Int ) -> Double? {
		return columnIsNull( column ) ? nil : columnDouble( column )
	}
	
	public func columnString( column:Int ) -> String? {
		let order = Int32(column)
		let utf8 = sqlite3_column_text( statement, order )
		
		return ( nil == utf8 ) ? nil : String( UTF8String:UnsafePointer<Int8>(utf8) )
	}
		
	public func columnString16( column:Int ) -> String? {
		let order = Int32(column)
		let utf16 = sqlite3_column_text16( statement, order )
		let length = sqlite3_column_bytes( statement, order )
		
		return ( nil == utf16 ) ? nil : String( utf16CodeUnits:UnsafePointer<UInt16>(utf16), count:Int(length)/sizeof(UInt16) )
	}
	
	public func columnStringObject( column:Int ) -> NSString? {
		let order = Int32(column)
		let utf8 = sqlite3_column_text( statement, order )
		let length = sqlite3_column_bytes( statement, order )
		
		return ( nil == utf8 ) ? nil : NSString( bytes:utf8, length:Int(length), encoding:NSUTF8StringEncoding )
	}
	
	public func columnData( column:Int ) -> NSData? {
		let order = Int32(column)
		let blob = sqlite3_column_blob( statement, order )
		let length = sqlite3_column_bytes( statement, order )
		
		return ( nil == blob || 0 == length ) ? nil : NSData(bytes: blob, length: Int(length))
	}
	
	public func columnArray<T where T:BinaryRepresentable>( column:Int ) -> [T] {
		let order = Int32(column)
		let blob = sqlite3_column_blob( statement, order )
		let length = Int(sqlite3_column_bytes( statement, order ))
		let size = sizeof(T)
		
		guard size > 0 && length > 0 && nil != blob else { return [T]() }
		
		return Array<T>( UnsafeBufferPointer<T>( start:UnsafePointer<T>(blob), count:length / size ) )
	}
	
	public func columnBindableValue( column:Int ) -> BindableValue {
		let type = columnType( column )
		
		switch type {
		case .Null: return .Null
		case .Boolean: return .Boolean( columnBoolean( column ) )
		case .Long: return .Long( columnLong( column ) )
		case .Real: return .Real( columnDouble( column ) )
		case .Text: return .Text( columnString( column ) ?? "" )
		case .Data: return .Data( columnData( column ) ?? NSData() )
		}
	}
	
	public func columnValue<T>( column:Int ) -> T? {
		if let _ = 0.0 as? T { return columnDouble( column ) as? T }
		else if let _ = Int64(0) as? T { return columnLong( column ) as? T }
		else if let _ = 0 as? T { return columnInteger( column ) as? T }
		else if let _ = false as? T { return columnBoolean( column ) as? T }
		else if let _ = "" as? T { return columnString( column ) as? T }
		else if let _ = NSData() as? T { return columnData( column ) as? T }
		else { return nil }
	}
	
	public func columnObject( column:Int, null:AnyObject, pool:NSMutableSet? = nil ) -> AnyObject {
		var result:AnyObject
		let type = columnType( column )
		
		switch ( type ) {
		case .Boolean: result = columnBoolean( column )
		case .Long: result = NSNumber( longLong:columnLong( column ) )
		case .Real: result = columnDouble( column )
		case .Text: result = columnStringObject( column ) ?? null
		case .Data: result = columnData( column ) ?? null
		default: result = null
		}
		
		if null !== result {
			if let pool = pool {
				if let prior = pool.member( result ) { result = prior }
				else { pool.addObject( result ) }
			}
		}
		
		return result
	}
	
	//	MARK: Single Column Convenience
	
	public func oneColumn( inout values:[Int] ) throws { while try advance() { values.append( columnInteger( 0 ) ) } }
	public func oneColumn( inout values:[Double] ) throws { while try advance() { values.append( columnDouble( 0 ) ) } }
	public func oneColumn( inout values:[String], null:String = "" ) throws { while try advance() { values.append( columnString( 0 ) ?? null ) } }
	public func oneColumn( inout values:[NSData], null:NSData = NSData() ) throws { while try advance() { values.append( columnData( 0 ) ?? null ) } }
	
	//	MARK: Convenience
	
	public func columnMajorResults( null:AnyObject = NSNull() ) throws -> [[AnyObject]] {
		let count = columnCount
		var index:Int
		var result = [[AnyObject]]( count:count, repeatedValue:[AnyObject]() )
		let pool = NSMutableSet()
		
		while ( try advance() ) {
			for ( index = 0 ; index < count ; ++index ) {
				result[index].append( columnObject( index, null:null, pool:pool ) )
			}
		}
		
		pool.removeAllObjects()
		
		return result
	}
	
	func columnMajorResults( inout columns:[QueryAppendable], preferTypes:[QueryValue]? = nil ) throws -> [QueryAppendable] {
		guard try advance() else { return columns }
		//	columnType only meaningful after first advance
		
		let count = columnCount
		let limit = preferTypes?.count ?? 0
		let known = columns.count
		var index:Int
		var types = [QueryValue]( count:count, repeatedValue:.Null )
		
		let pool = NSMutableSet()
		let stringNull = NSString()
		let dataNull = NSData()
		let null = NSNull()
		
		for ( index = known ; index-- > count ;  ) {
			columns.removeAtIndex( index )
		}
		
		for ( index = 0 ; index < count ; ++index ) {
			if index < known {
				if let prefers = columns[index].prefersQueryValue {
					columns[index].removeAll( keepCapacity:true )
					types[index] = prefers
					continue
				}
			}
			
			let preferType = ( index < limit ) ? preferTypes![index] : .Null
			let type = ( .Null == preferType ) ? columnType( index ) : preferType
			var column:QueryAppendable
			
			switch type {
			case .Boolean: column = [Bool]()
			case .Long: column = [Int64]()
			case .Real: column = [Double]()
			case .Text: column = [NSString]()
			case .Data: column = [NSData]()
			case .Null: column = [AnyObject]()
			}
			
			if index < columns.count { columns[index] = column }
			else { columns.append( column ) }
			types[index] = type
		}
		
		repeat {
			for ( index = 0 ; index < count ; ++index ) {
				switch types[index] {
				case .Boolean: columns[index].queryAppend( columnBoolean( index ) )
				case .Long: columns[index].queryAppend( columnLong( index ) )
				case .Real: columns[index].queryAppend( columnDouble( index ) )
				case .Text: columns[index].queryAppend( columnStringObject( index ) ?? stringNull )
				case .Data: columns[index].queryAppend( columnData( index ) ?? dataNull )
				case .Null: columns[index].queryAppend( columnObject( index, null:null, pool:pool ) )
				}
			}
		} while try advance()
		
		return columns
	}
	
	//	MARK: Statement Management
	
	public static func prepare( inout statement:COpaquePointer, sql:String, connection:COpaquePointer ) -> (code:Int32,String?) {
		var code:Int32
		var message:String?
		
		if nil == connection {
			code = SQLITE_MISUSE
		} else {
			let utf8 = ( sql as NSString ).UTF8String
			code = sqlite3_prepare_v2( connection, utf8, -1, &statement, nil )
		}
		
		if nil == statement {
			message = ( nil == connection ) ? "connection required" : Connection.errorDescription( code, connection:connection ) ?? "prepare error"
		} else {
			message = nil
		}
		
		return (code,message)
	}
	
	private func prepareThrows( sql:String, connection:COpaquePointer ) throws -> (code:Int32,String?) {
		let (code,string) = Statement.prepare( &statement, sql:sql, connection:connection )
		
		status = code
		message = string
		
		if !( SQLITE_OK == code || SQLITE_NOTICE == code || SQLITE_WARNING == code ) {
			if let description = string {
				throw Error( code:code, message:description, detail:sql )
			}
		}
		
		return (code,string)
	}
}

extension Statement : SequenceType, GeneratorType {
	public typealias Generator = Statement
	public typealias Element = QueryRow
	
	public func generate() -> Generator { return self }
	public func next() -> Element? { do { return try advance() ? self : nil } catch { return nil } }
	public func another() throws -> QueryRow? { return try advance() ? self : nil }
	public func underestimateCount() -> Int { return 0 }
	
    public func forEach( @noescape body:(Element) throws -> Void ) throws {
		while try advance() { try body( self ) }
	}
	
	public func map<T>( @noescape transform:(Element) throws -> T) throws -> [T] {
		var result = [T]()
		
		while try advance() { result.append( try transform( self ) ) }
		
		return result
	}
	
	public func filter<T>( @noescape transform:(Element) -> T?) throws -> [T] {
		var result = [T]()
		
		while try advance() { if let t = transform( self ) { result.append( t ) } }
		
		return result
	}
	
	public func filter( @noescape includeElement:(Element) -> Bool ) throws -> [Element] {
		throw Error( code:SQLITE_MISUSE, message:"filter unsupported", detail:"" )
	}
}

extension Statement : QueryRow {
	public subscript( column:Int ) -> Bool { return columnBoolean( column ) }
	public subscript( column:Int ) -> Int64 { return columnLong( column ) }
	public subscript( column:Int ) -> Double { return columnDouble( column ) }
	public subscript( column:Int ) -> String? { return columnString( column ) }
	public subscript( column:Int ) -> NSData? { return columnData( column ) }
}

//	MARK: - Bindable Extensions

extension QueryValue {
	static func forType<T>( value:T? ) -> QueryValue? {
		if value is Any.Type {
			if Double.self is T { return .Real }
			if Int64.self is T { return .Long }
			if Bool.self is T { return .Boolean }
			if AnyObject.self is T || NSObject.self is T { return .Null }
			if String.self is T || NSString.self is T { return .Text }
			if NSData.self is T { return .Data }
		} else {
			if Double.self is T.Type { return .Real }
			if Int64.self is T.Type { return .Long }
			if Bool.self is T.Type { return .Boolean }
			if AnyObject.self is T.Type || NSObject.self is T.Type { return .Null }
			if String.self is T.Type || NSString.self is T.Type { return .Text }
			if NSData.self is T.Type { return .Data }
		}
		return nil
	}
}

func toVoid( memory:UnsafePointer<Void> ) -> UnsafePointer<Void> { return memory }

func isBinaryRepresentable<T>( it:T? = nil ) -> Bool {
	if T.self is AnyObject { return false }
	if T.self is _PointerType.Type { return false }
	if T.self is BinaryRepresentable.Type { return true }
	
	let deepType = "\(T.self.dynamicType)"
	
	if deepType.hasSuffix( ").Type" ) { return false }	//	tuple or closure
	if deepType.hasSuffix( ".Protocol" ) { return false }
	if deepType.hasSuffix( ".Type.Type" ) { return false }
	
	if let t = it {
		if let displayStyle = Mirror(reflecting:t).displayStyle {
			if Mirror.DisplayStyle.Struct != displayStyle { return false }
		}
	} else {
		if deepType.hasPrefix( "Swift.Optional<" ) { return false }
		if deepType.hasPrefix( "Swift.ImplicitlyUnwrappedOptional<" ) { return false }
	}
	
	return true
}

extension BindableValue : Bindable { public var bindableValue:BindableValue { return self } }
extension Bool : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Boolean(self) } }

extension Int : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(self) } }
extension UInt : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension Int8 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension UInt8 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension Int16 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension UInt16 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension Int32 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension UInt32 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Integer(Int(self)) } }
extension Int64 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Long(self) } }
extension UInt64 : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Long(Int64(self)) } }

extension Double : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Real(self) } }
extension Float : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Real(Double(self)) } }

extension NSNull : Bindable { public var bindableValue:BindableValue { return .Null } }
extension NSDate : Bindable { public var bindableValue:BindableValue { return .Real(self.timeIntervalSince1970) } }

extension String : Bindable {
	public var bindableValue:BindableValue { return .UTF8(toVoid(self),byteCount:self.utf8.count) }
	public var bindableText:(utf8:UnsafePointer<Void>,byteCount:Int) { return (toVoid(self),self.utf8.count) }
}

extension NSNumber : Bindable {
	public var bindableValue:BindableValue { return CFNumberIsFloatType( self ) ? BindableValue.Real(self.doubleValue) : BindableValue.Long(self.longLongValue) }
}

extension NSString : Bindable {
	public var bindableValue:BindableValue { let utf8 = UTF8String; return .UTF8(toVoid(utf8),byteCount:Int(strlen(utf8))) }
	public var bindableText:(utf8:UnsafePointer<Void>,byteCount:Int) { let utf8 = UTF8String; return (toVoid(utf8),Int(strlen(utf8))) }
}

extension NSData : Bindable {
	public var bindableValue:BindableValue { return .Blob(bytes,byteCount:length) }
	public var bindableBlob:(data:UnsafePointer<Void>,byteCount:Int) { return (bytes,byteCount:length) }
	public var bindableString:NSString { return NSString( data:self, encoding:NSUTF8StringEncoding )! }
}

extension Array : Bindable /* where Element : BinaryRepresentable */ {
	public var bindableSize:Int { return self.count*sizeof(Element.self) }
	public var bindableValue:BindableValue { return bindableSize > 0 && Element.self is BinaryRepresentable.Type ? .Blob(toVoid(self),byteCount:bindableSize) : .Null }
}

extension UnsafeBufferPointer : Bindable {
	public var bindableSize:Int { return self.count*sizeof(Element.self) }
	public var bindableValue:BindableValue { return .Blob(self.baseAddress,byteCount:bindableSize) }
}

extension UnsafePointer : Bindable {
	public var bindableValue:BindableValue { return .Blob(self,byteCount:sizeof(Memory.self)) }
}

public struct BindableZeroData : Bindable {
	public let length:Int32
	public var bindableValue:BindableValue { return .Zero(byteCount:Int32(length)) }
}

extension Array : QueryAppendable {
	public var prefersQueryValue:QueryValue? { return QueryValue.forType( first ) }
	mutating public func queryAppend<T>( value:T ) { append( value as! Element ) }
}
extension Set : QueryAppendable {
	public var prefersQueryValue:QueryValue? { return QueryValue.forType( first ) }
	mutating public func queryAppend<T>( value:T ) { insert( value as! Element ) }
}


private let SQLITE_BOOL:Int32 = 8
private let SQLITE_STATIC = unsafeBitCast( intptr_t(0), sqlite3_destructor_type.self )
private let SQLITE_TRANSIENT = unsafeBitCast( intptr_t(-1), sqlite3_destructor_type.self )

/*
import CoreGraphics

extension CGFloat : Bindable, BinaryRepresentable { public var bindableValue:BindableValue { return .Real(Double(self)) } }
extension CGSize : BinaryRepresentable {}
extension CGPoint : BinaryRepresentable {}
extension CGRect : BinaryRepresentable {}
extension CGVector : BinaryRepresentable {}
extension CGAffineTransform : BinaryRepresentable {}
*/

