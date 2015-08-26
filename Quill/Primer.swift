//
//  Primer.swift
//  Quill
//
//  Created by Cole, Eric (ETW) on 8/23/15.
//  Copyright © 2015 Cole, Eric (ETW). All rights reserved.
//

import Foundation

struct ColumnDeclaration {
	static let Boolean = "BOOLEAN"	//	BOOL
	static let Integer = "INTEGER"	//	INT LONG NUMBER ...
	static let Real = "REAL"		//	FLOAT DOUBLE NUMERIC
	static let Text = "TEXT"		//	CHAR CLOB VARCHAR
	static let Untyped = "BLOB"		//	NONE
	
	static let CollateBinary = "BINARY"
	static let CollateCaseInsensitive = "NOCASE"
	static let CollateIgnoreTrailingSpace = "RTRIM"
	
	enum Conflict : Int, CustomStringConvertible {
		case Yes = 0, Rollback = 1, Abort = 2, Fail = 3, Ignore = 4, Replace = 5
		var description:String { return ["default","rollback","abort","fail","ignore","replace"][rawValue] }
		var sql:String { return ( .Yes != self ) ? " on conflict " + description : "" }
	}
	
	///	http://sqlite.org/syntax/column-constraint.html
	let name:String
	var type:String?	//	nil is NUMERIC
	var primaryKey:Conflict?
	var primaryDescending:Bool?
	var primaryAutoincrement:Bool = false
	var notNull:Conflict?
	var unique:Conflict?
	var check:String?
	var defaults:String?
	var collate:String?
	var foreignKey:String?
	
	var sql:String {
		let tp = ( nil != type ) ? " " + type! : ""
		let pk = ( nil != primaryKey ) ? " primary key" + ( nil != primaryDescending ? primaryDescending! ? " desc" : " asc" : "" ) + primaryKey!.sql + ( primaryAutoincrement ? " autoincrement" : "" ) : ""
		let nn = ( nil != notNull ) ? " not null" + notNull!.sql : ""
		let uq = ( nil != unique ) ? " unique" + unique!.sql : ""
		let ck = ( nil != check ) ? " check ( " + check! + " )" : ""
		let df = ( nil != defaults ) ? " default " + defaults! : ""
		let cl = ( nil != collate ) ? " collate " + collate! : ""
		let fk = ( nil != foreignKey ) ? " references " + foreignKey! : ""
		
		return name + tp + pk + nn + uq + ck + df + cl + fk
	}
	
	init( name:String ) {
		self.name = name
	}
	
	init( name:String, type:String?, defaults:String? = nil, notNull:Conflict? = nil, unique:Conflict? = nil, primaryKey:Conflict? = nil, primaryDescending:Bool? = nil, primaryAutoincrement:Bool = false, check:String? = nil, collate:String? = nil, foreignKey:String? = nil ) {
		self.name = name
		self.type = type
		self.primaryKey = primaryKey
		self.primaryDescending = primaryDescending
		self.primaryAutoincrement = primaryAutoincrement
		self.notNull = notNull
		self.unique = unique
		self.check = check
		self.defaults = defaults
		self.collate = collate
		self.foreignKey = foreignKey

		if nil == type && nil == defaults && nil != primaryKey && !( primaryDescending ?? false ) {
			self.type = "INTEGER"	//	special integer primary key
		}
	}
	
	static func boolean( name:String, defaults:Bool = false ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:Boolean, defaults:( defaults ? "1" : "0" ), notNull:.Yes )
	}
	
	static func integer( name:String, defaults:Int? = 0, unique:Bool = false ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:Integer, defaults:( nil == defaults ? nil : String(defaults!) ), notNull:( nil == defaults ? nil : .Yes ), unique:( unique ? .Yes : nil ) )
	}
	
	static func real( name:String, defaults:Double? = 0, unique:Bool = false ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:Real, defaults:( nil == defaults ? nil : String(defaults!) ), notNull:( nil == defaults ? nil : .Yes ), unique:( unique ? .Yes : nil ) )
	}
	
	static func text( name:String, defaults:String? = "", unique:Bool = false, collate:String? ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:Text, defaults:( nil == defaults ? nil : Connection.escape( defaults! ) ), notNull:( nil == defaults ? nil : .Yes ), unique:( unique ? .Yes : nil ), primaryKey:nil, primaryDescending:nil, primaryAutoincrement:false, check:nil, collate:collate )
	}
	
	static func data( name:String, unique:Bool = false ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:Untyped, defaults:nil, notNull:nil, unique:( unique ? .Yes : nil ) )
	}
	
	static func primaryKey( name:String, type:String? = Integer, descending:Bool? = nil ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:type, defaults:nil, notNull:.Yes, unique:nil, primaryKey:.Yes, primaryDescending:descending, primaryAutoincrement:false )
	}
	
	static func foriegnKey( name:String, foreignKey:String, notNull:Bool = false ) -> ColumnDeclaration {
		return ColumnDeclaration( name:name, type:nil, defaults:nil, notNull:( notNull ? .Yes : nil ), unique:nil, primaryKey:nil, primaryDescending:nil, primaryAutoincrement:false, check:nil, collate:nil, foreignKey:foreignKey )
	}
	
	static func join( columns:[ColumnDeclaration] ) -> String {
		var result = ""
		var index = 0
		
		for column in columns {
			if index++ > 0 { result += ", " }
			result += column.sql
		}
		
		return result
	}
}

extension Connection {
	// escape string literal as 'text' where all ' in text are doubled
	// escape data literal as x'hexadecimal'
	
	static func escape( string:String ) -> String {
		return "'" + string.stringByReplacingOccurrencesOfString( "'", withString:"''" ) + "'"
	}
	
	///	http://sqlite.org/lang_select.html
	///	http://sqlite.org/lang_insert.html
	///	http://sqlite.org/lang_update.html
	///	http://sqlite.org/lang_delete.html
	///	http://sqlite.org/lang_with.html
	
	///	http://sqlite.org/lang_transaction.html
	enum TransactionType : Int { case Default = 0, Deferred = 1, Immediate = 2, Exclusive = 3 }
	func beginTransaction( type:TransactionType = .Default ) throws -> Bool {
		let modifier = [""," deferred"," immediate"," exclusive"][type.rawValue]
		
		return try execute( "begin" + modifier + " transaction" )
	}
	
	///	http://sqlite.org/lang_transaction.html
	func commit() throws -> Bool {
		return try execute( "commit transaction" )
	}
	
	///	http://sqlite.org/lang_transaction.html
	func rollback( savepoint:String? = nil ) throws -> Bool {
		return try execute( "rollback transaction" + ( nil != savepoint ? " to savepoint " + savepoint! : "" ) )
	}
	
	///	http://sqlite.org/lang_savepoint.html
	func makeSavepoint( savepoint:String ) throws -> Bool {
		return try execute( "savepoint " + savepoint )
	}
	
	///	http://sqlite.org/lang_savepoint.html
	func freeSavepoint( savepoint:String ) throws -> Bool {
		return try execute( "release savepoint " + savepoint )
	}
	
	///	http://sqlite.org/lang_attach.html
	public static let LocationMemory = "':memory:'"
	public static let LocationTemporary = ""
	func attachDatabase( name:String , location:String = LocationMemory ) throws -> Bool {
		return try execute( "attach database " + location + " as " + name )
	}
	
	///	http://sqlite.org/lang_detach.html
	func detachDatabase( name:String ) throws -> Bool {
		return try execute( "detach database " + name )
	}
	
	///	http://sqlite.org/lang_reindex.html
	func reindex( name:String ) throws -> Bool {
		return try execute( "reindex " + name )
	}
	
	///	http://sqlite.org/lang_createindex.html
	func createIndex( name:String, onTable:String, columnNames:String, unique:Bool = false, whereCondition:String? = nil ) throws -> Bool {
		return try execute( "create" + ( unique ? " unique" : "" ) + " index" + ( " if not exists" ) + " " + name + " on " + onTable + " (" + columnNames + ")" + ( nil != whereCondition ? " where " + whereCondition! : "" ) )
	}
	
	///	http://sqlite.org/lang_dropindex.html
	func dropIndex( name:String ) throws -> Bool {
		return try execute( "drop index if exists " + name )
	}
	
	///	http://sqlite.org/lang_createtable.html
	func createTable( name:String, columnDeclarations:String, temporary:Bool = false, strict:Bool = false, withRowIdentifiers:Bool = true ) throws -> Bool {
		return try execute( "create" + ( temporary ? " temporary" : "" ) + " table" + ( strict ? "" : " if not exists" ) + " " + name + " (" + columnDeclarations + ")" + ( withRowIdentifiers ? "" : " without rowid" ) )
	}
	
	///	http://sqlite.org/lang_createtable.html
	func createTableAs( name:String, select:String, temporary:Bool = false, strict:Bool = false ) throws -> Bool {
		return try execute( "create" + ( temporary ? " temporary" : "" ) + " table" + ( strict ? "" : " if not exists" ) + " " + name + " as " + select )
	}
	
	///	http://sqlite.org/lang_altertable.html
	func createTableColumn( name:String, columnDeclaration:String ) throws -> Bool {
		return try execute( "alter table " + name + " add column " + columnDeclaration )
	}
	
	///	http://sqlite.org/lang_altertable.html
	func renameTable( name:String, newName:String ) throws -> Bool {
		return try execute( "alter table " + name + " rename to " + newName )
	}
	
	///	http://sqlite.org/lang_delete.html
	func deleteAll( name:String ) throws -> Bool {
		return try execute( "delete from " + name )
	}
	
	///	http://sqlite.org/lang_droptable.html
	func dropTable( name:String ) throws -> Bool {
		return try execute( "drop table if exists " + name )
	}
	
	///	http://sqlite.org/lang_createview.html
	func createView( name:String, select:String, temporary:Bool = false, strict:Bool = false ) throws -> Bool {
		return try execute( "create" + ( temporary ? " temporary" : "" ) + " view" + ( strict ? "" : " if not exists" ) + " " + name + " as " + select )
	}
	
	///	http://sqlite.org/lang_dropview.html
	func dropView( name:String ) throws -> Bool {
		return try execute( "drop view if exists " + name )
	}
	
	///	http://sqlite.org/lang_createtrigger.html
	enum TriggerEvent { case Delete, Insert, Update, UpdateOf(columns:String) }
	enum TriggerPhase : Int { case Default = 0, Before = 1, After = 2, Instead = 3 }
	func createTrigger( name:String, table:String, event:TriggerEvent, statement:String, phase:TriggerPhase = .Default, when:String? = nil, temporary:Bool = false, strict:Bool = false ) throws -> Bool {
		let tt = ( temporary ? " temporary" : "" )
		let tx = ( strict ? "" : " if not exists" )
		let te:String
		let tp = [""," before"," after"," instead of"][phase.rawValue]
		let tf = " for each row"
		let tw = ( nil != when ? " when " + when! : "" )
		
		switch ( event ) {
		case .Delete: te = " delete"
		case .Insert: te = " insert"
		case .Update: te = " update"
		case .UpdateOf(let columns): te = " update of " + columns
		}
		
		return try execute( "create" + tt + " trigger" + tx + " " + name + tp + te + " on " + table + tf + tw + " begin " + statement + "; end" )
	}
	
	///	http://sqlite.org/lang_droptrigger.html
	func dropTrigger( name:String ) throws -> Bool {
		return try execute( "drop trigger if exists " + name )
	}
	
	///	http://sqlite.org/lang_vacuum.html
	func vacuum() throws -> Bool {
		return try execute( "vacuum" )
	}
	
	///	http://sqlite.org/lang_analyze.html
	func analyze( name:String ) throws -> Bool {
		return try execute( "analyze " + name )
	}
	
	///	http://sqlite.org/lang_explain.html
}

