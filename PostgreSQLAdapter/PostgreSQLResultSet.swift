//
//  PostgreSQLResultSet.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/10/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import DatabaseAdapter
import libpq

public class PostgreSQLResultSet: ResultSet {
    let adapter: PostgreSQLAdapter
    let pgRes: COpaquePointer?
    let singleRowMode: Bool
    var error: ErrorType?
    
    init(adapter: PostgreSQLAdapter) {
        self.adapter = adapter
        self.pgRes = nil
        self.singleRowMode = false
    }
    
    init(adapter: PostgreSQLAdapter, pgRes: COpaquePointer) {
        self.adapter = adapter
        self.pgRes = pgRes
        self.singleRowMode = false
    }
    
    init(adapter: PostgreSQLAdapter, singleRowMode: Bool) {
        self.adapter = adapter
        self.pgRes = nil
        self.singleRowMode = singleRowMode
    }
    
    deinit {
        if let pgRes = self.pgRes {
            PQclear(pgRes)
        }
    }
    
    public var rowCount: Int {
        guard let pgRes = self.pgRes else {
            return 0
        }
        
        return Int(PQntuples(pgRes))
    }
    
    public var columnNames: [String] {
        guard let pgRes = self.pgRes else {
            return []
        }
        
        return getColumnNames(pgRes)
    }
    
    private func getColumnNames(result: PostgresResult) -> [String] {
        let fieldCount = PQnfields(result)
        return (0..<fieldCount).map({ i in
            if let fieldName = String(CString: PQfname(result, i), encoding: NSUTF8StringEncoding) {
                return fieldName
            } else {
                return ""
            }
        })
    }
    
    private func castColumnValue(result: PostgresResult, rowIndex: Int32, columnIndex: Int32) -> AnyObject? {
        if PQgetisnull(result, rowIndex, columnIndex) == 0 {
            return String(CString: PQgetvalue(result, rowIndex, columnIndex), encoding: NSUTF8StringEncoding)
        } else {
            return nil
        }
    }
    
    private func buildResultRow(columnNames: [String], result: PostgresResult, rowIndex: Int32) -> ResultRow {
        var columnValues: [AnyObject?] = []
        for i: Int32 in Int32(0)..<Int32(columnNames.count) {
            columnValues.append(self.castColumnValue(result, rowIndex: rowIndex, columnIndex: i))
        }
        
        return ResultRow(columnNames: columnNames, columnValues: columnValues)
    }
    
    private func rowsFromResult() -> AnyGenerator<ResultRow> {
        guard let pgRes = self.pgRes else {
            return anyGenerator({ () -> ResultRow? in
                return nil
            })
        }
        
        var rowIndex = Int32(0)
        let rowCount = Int32(self.rowCount)
        let columnNames = self.columnNames
        
        return anyGenerator({ () -> ResultRow? in
            if rowIndex >= rowCount {
                return nil
            }
            
            let resultRow = self.buildResultRow(columnNames, result: pgRes, rowIndex: rowIndex)
            rowIndex += 1
            return resultRow
        })
    }
    
    private func rowsFromSingleRowMode() -> AnyGenerator<ResultRow> {
        return anyGenerator({ () -> ResultRow? in
            let result = PQgetResult(self.adapter.pgConn)
            if result == PostgresResult(nilLiteral: ()) {
                return nil
            }
            
            defer {
                PQclear(result)
            }
            
            do {
                return try self.adapter.handleExecResult(result, successCallback: { (result, status) -> ResultRow? in
                    if status == PGRES_TUPLES_OK {
                        // we're done; flush the remaining results and return nil
                        var ignorableResult = PQgetResult(self.adapter.pgConn)
                        while ignorableResult != PostgresResult(nilLiteral: ()) {
                            ignorableResult = PQgetResult(self.adapter.pgConn)
                        }
                        
                        return nil
                    } else {
                        return self.buildResultRow(self.getColumnNames(result), result: result, rowIndex: 0)
                    }
                })
            } catch {
                self.error = error
                return nil
            }
        })
    }
    
    public func rows() -> AnyGenerator<ResultRow> {
        if singleRowMode {
            return rowsFromSingleRowMode()
        } else {
            return rowsFromResult()
        }
    }
}