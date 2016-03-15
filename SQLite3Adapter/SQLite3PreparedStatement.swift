//
//  SQLite3PreparedStatement.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 2/18/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import libsqlite3

typealias SQLite3PreparedStatementHandle = COpaquePointer

public class SQLite3PreparedStatement {
    let adapter: SQLite3Adapter
    let handlePtr = UnsafeMutablePointer<SQLite3PreparedStatementHandle>.alloc(1)
    var columnNamesMemoized: [String]?
    
    var handle: SQLite3PreparedStatementHandle { return handlePtr.memory }

    init(adapter: SQLite3Adapter, sql: String) throws {
        self.adapter = adapter
        
        guard let sqlCString = sql.cStringUsingEncoding(NSUTF8StringEncoding) else {
            throw SQLite3Adapter.Error.CannotEncodeString
        }
    
        if sqlite3_prepare_v2(adapter.connection, sqlCString, Int32(sqlCString.count), handlePtr, nil) != SQLITE_OK {
            throw adapter.lastError
        }
    }
    
    deinit {
        sqlite3_finalize(handle)
    }
    
    var columnNames: [String] {
        if columnNamesMemoized == nil {
            let columnCount = sqlite3_column_count(handle)
            
            self.columnNamesMemoized = (Int32(0)..<columnCount).map({ index in
                let columnNameCString = sqlite3_column_name(handle, index)
                guard let columnName = String(CString: columnNameCString, encoding: NSUTF8StringEncoding) else {
                    return "UNKNOWN_COLUMN_\(index)"
                }
                
                return columnName
            })
        }
        
        return columnNamesMemoized!
    }
    
    func step() -> Int32 {
        return sqlite3_step(handle)
    }
    
    func dataCount() -> Int32 {
        return sqlite3_data_count(handle)
    }
    
    func columnValue(index: Int32) -> Any? {
        let columnType = sqlite3_column_type(handle, index)
        
        switch (columnType) {
        case SQLITE_NULL:
            return nil
        case SQLITE_INTEGER:
            return sqlite3_column_int64(handle, index)
        case SQLITE_FLOAT:
            return sqlite3_column_double(handle, index)
        case SQLITE_TEXT:
            let text = sqlite3_column_text(handle, index)
            let data = NSData(bytes: text, length: Int(sqlite3_column_bytes(handle, index)))
            let str = String(data: data, encoding: NSUTF8StringEncoding)
            return str
        case SQLITE_BLOB:
            let blob = sqlite3_column_blob(handle, index)
            return NSData(bytes: blob, length: Int(sqlite3_column_bytes(handle, index)))
        default:
            return nil
        }
    }
    
    func reset() {
        sqlite3_reset(handle)
    }
}