//
//  SQLite3Adapter.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 2/18/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import libsqlite3
import DatabaseAdapter

typealias SQLite3Connection = COpaquePointer

public class SQLite3Adapter: DatabaseAdapter {
    enum Error: ErrorType {
        case NotConnected
        case CannotEncodeString
        case SQLiteError(Int32, String)
    }
    
    let connectionPtr: UnsafeMutablePointer<SQLite3Connection>
    var connection: SQLite3Connection { return connectionPtr.memory }
    
    var lastError: Error {
        if connection == COpaquePointer(nilLiteral: ()) {
            return Error.NotConnected
        } else {
            let code = sqlite3_errcode(connection)
            if let msg = String(CString: sqlite3_errmsg(connection), encoding: NSUTF8StringEncoding) {
                return Error.SQLiteError(code, msg)
            } else {
                return Error.SQLiteError(code, "Could not retrieve error message from SQLite")
            }
        }
    }
    
    public var tableNames: [String] {
        let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"

        do {
            let tableNames = try select(sql).rows().map({ (row) in return row["name"] as! String })
            return tableNames
        } catch {
            return []
        }
    }
    
    public init(filename: String) throws {
        connectionPtr = UnsafeMutablePointer<COpaquePointer>.alloc(1)

        guard let filenameCString = filename.cStringUsingEncoding(NSUTF8StringEncoding) else {
            throw Error.CannotEncodeString
        }
        
        if sqlite3_open(filenameCString, connectionPtr) != SQLITE_OK {
            throw lastError
        }
    }
    
    deinit {
        sqlite3_close(connection)
    }
    
    public func prepare(sql: String) throws -> SQLite3PreparedStatement {
        return try SQLite3PreparedStatement(adapter: self, sql: sql)
    }
    
    public func select(sql: String) throws -> ResultSet {
        let statement = try prepare(sql)
        return SQLite3ResultSet(preparedStatement: statement)
    }
    
    public func execute(sql: String) throws -> Int {
        let resultSet = try select(sql)
        
        for _ in resultSet.rows() {
            // just exhaust the result set
        }
        
        return Int(sqlite3_changes(connection))
    }
    
    public func getTable(name: String) throws -> Table {
        return SQLite3Table(adapter: self, name: name)
    }
}
