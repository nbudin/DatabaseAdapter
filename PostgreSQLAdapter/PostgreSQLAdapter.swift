//
//  PostgreSQLAdapter.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/9/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import DatabaseAdapter
import libpq

typealias PostgresConnection = COpaquePointer
typealias PostgresResult = COpaquePointer

public class PostgreSQLAdapter: DatabaseAdapter {
    public struct ConnectionParameters {
        let host: String
        let port: String
        let user: String
        let pass: String
        let dbname: String
    }
    
    public enum ErrorStatus {
        case Fatal
        case Nonfatal
    }
    
    public enum Error: ErrorType {
        case ConnectionError(String)
        case EmptyQuery
        case NoResults
        case BadResponse
        case UnknownError(String)
        case QueryError(String, ErrorStatus)
        case QuerySendError(String)
        case StringEscapingError(String)
        case UnknownStatusCode(ExecStatusType)
    }
    
    enum UnimplementedError: ErrorType {
        case Unimplemented
    }
    
    let connectionParameters: ConnectionParameters
    let pgConn: PostgresConnection
    
    public init(paramsDict: [String : String]) throws {
        let paramsDictKeys = Array(paramsDict.keys)
        let paramsDictValues = paramsDictKeys.map({ key in
            paramsDict[key]!
        })
        
        pgConn = try! paramsDictKeys.withCStringArray({ keywords in
            return try! paramsDictValues.withCStringArray({ values in
                return PQconnectdbParams(keywords, values, 0)
            })
        })
        
        let host = String.fromCString(PQport(pgConn), encoding: NSUTF8StringEncoding, defaultValue: "")
        let port = String.fromCString(PQport(pgConn), encoding: NSUTF8StringEncoding, defaultValue: "")
        let user = String.fromCString(PQuser(pgConn), encoding: NSUTF8StringEncoding, defaultValue: "")
        let pass = String.fromCString(PQpass(pgConn), encoding: NSUTF8StringEncoding, defaultValue: "")
        let dbname = String.fromCString(PQdb(pgConn), encoding: NSUTF8StringEncoding, defaultValue: "")
        
        self.connectionParameters = ConnectionParameters(host: host, port: port, user: user, pass: pass, dbname: dbname)
        
        if PQstatus(pgConn) == CONNECTION_BAD {
            guard let errorMessage = String(CString: PQerrorMessage(pgConn), encoding: NSUTF8StringEncoding) else {
                throw Error.ConnectionError("Cannot retrieve error message from PostgreSQL client")
            }
            
            throw Error.ConnectionError(errorMessage)
        }
    }
    
    deinit {
        PQfinish(pgConn)
    }
    
    private func getErrorMessage() throws -> String {
        guard let msg = String(CString: PQerrorMessage(pgConn), encoding: NSUTF8StringEncoding) else {
            throw Error.UnknownError("Could not retrieve error message from server")
        }
        return msg
    }
    
    internal func handleExecResult<ResultType>(result: PostgresResult, successCallback: (result: PostgresResult, status: ExecStatusType) -> ResultType) throws -> ResultType {
        
        let status = PQresultStatus(result)
        switch status {
        case PGRES_EMPTY_QUERY:
            throw Error.EmptyQuery
        case PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_SINGLE_TUPLE, PGRES_COPY_OUT, PGRES_COPY_IN, PGRES_COPY_BOTH:
            return successCallback(result: result, status: status)
        case PGRES_BAD_RESPONSE:
            throw Error.BadResponse
        case PGRES_NONFATAL_ERROR, PGRES_FATAL_ERROR:
            let errorStatus = (status == PGRES_FATAL_ERROR) ? ErrorStatus.Fatal : ErrorStatus.Nonfatal
            throw try Error.QueryError(self.getErrorMessage(), errorStatus)
        default:
            throw Error.UnknownStatusCode(status)
        }
    }
    
    private func getLastResult() throws -> PostgresResult {
        var currentResult: PostgresResult = PQgetResult(pgConn)
        var lastResult: PostgresResult?
        
        while currentResult != PostgresResult(nilLiteral: ()) {
            lastResult = currentResult
            currentResult = PQgetResult(pgConn)
        }
        
        guard let theLastResult = lastResult else {
            throw Error.NoResults
        }
        
        return theLastResult
    }
    
    private func execSQL<ResultType>(sql: String, withSuccessfulResult: (result: PostgresResult, status: ExecStatusType) -> ResultType) throws -> ResultType {
        return try sql.withCString({ (cString) in
            if PQsendQuery(pgConn, cString) == 1 {
                let lastResult = try getLastResult()
                return try handleExecResult(lastResult, successCallback: withSuccessfulResult)
            } else {
                throw try Error.QuerySendError(self.getErrorMessage())
            }
        })
    }
    
    private func execSQL<ResultType>(sql: String, params: [String], withSuccessfulResult: (result: PostgresResult, status: ExecStatusType) -> ResultType) throws -> ResultType {
        
        return try sql.withCString({ (sqlCString) in
            return try params.withCStringArray({ (paramsArray) in
                if PQsendQueryParams(self.pgConn, sqlCString, Int32(params.count), nil, paramsArray, nil, nil, 0) == 1 {
                    let lastResult = try self.getLastResult()
                    return try self.handleExecResult(lastResult, successCallback: withSuccessfulResult)
                } else {
                    throw try Error.QuerySendError(self.getErrorMessage())
                }
            })
        })
    }
    
    private func execSQLSingleRowMode<ResultType>(sql: String, withSuccessfulResult: (result: ResultSet) -> ResultType) throws -> ResultType {
        return try sql.withCString({ (cString) in
            if PQsendQuery(pgConn, cString) == 1 {
                if PQsetSingleRowMode(pgConn) == 1 {
                    return withSuccessfulResult(result: PostgreSQLResultSet(adapter: self, singleRowMode: true))
                } else {
                    let lastResult = try self.getLastResult()
                    return try handleExecResult(lastResult, successCallback: { result in return withSuccessfulResult(result: PostgreSQLResultSet(adapter: self, pgRes: lastResult)) })
                }
            } else {
                throw try Error.QuerySendError(self.getErrorMessage())
            }
        })
    }
    
    private func execSQLSingleRowMode<ResultType>(sql: String, params: [String], withSuccessfulResult: (result: ResultSet) -> ResultType) throws -> ResultType {
        
        return try sql.withCString({ (sqlCString) in
            return try params.withCStringArray({ (paramsArray) in
                if PQsendQueryParams(self.pgConn, sqlCString, Int32(params.count), nil, paramsArray, nil, nil, 0) == 1 {
                    if PQsetSingleRowMode(self.pgConn) == 1 {
                        return withSuccessfulResult(result: PostgreSQLResultSet(adapter: self, singleRowMode: true))
                    } else {
                        let lastResult = try self.getLastResult()
                        return try self.handleExecResult(lastResult, successCallback: { result in return withSuccessfulResult(result: PostgreSQLResultSet(adapter: self, pgRes: lastResult)) })
                    }
                } else {
                    throw try Error.QuerySendError(self.getErrorMessage())
                }
            })
        })
    }
    
    public func execute(sql: String) throws -> Int {
        return try execSQL(sql, withSuccessfulResult: { result, _ in
            if let tuplesString = String(CString: PQcmdTuples(result), encoding: NSUTF8StringEncoding) {
                if let tuples = Int(tuplesString) {
                    return tuples
                } else {
                    return 0
                }
            } else {
                return 0
            }
        })
    }
    
    public func select(sql: String) throws -> ResultSet {
        return try execSQLSingleRowMode(sql, withSuccessfulResult: { resultSet in return resultSet })
    }
    
    public func select(sql: String, params: [String]) throws -> ResultSet {
        return try execSQLSingleRowMode(sql, params: params, withSuccessfulResult: { resultSet in return resultSet })
    }
    
    public func escapeString(string: String) throws -> String {
        let byteLength = string.lengthOfBytesUsingEncoding(NSUTF8StringEncoding)
        
        return try string.withCString({ stringPtr in
            let outBufferSize = byteLength * 2 + 1
            let outPtr = UnsafeMutablePointer<Int8>.alloc(outBufferSize)
            let errorPtr = UnsafeMutablePointer<Int32>.alloc(1)
            PQescapeStringConn(pgConn, outPtr, stringPtr, byteLength, errorPtr)
            
            if (errorPtr.memory != 0) {
                throw try Error.StringEscapingError(self.getErrorMessage())
            }
            
            guard let escapedString = String(CString: outPtr, encoding: NSUTF8StringEncoding) else {
                throw Error.StringEscapingError("Escaped string could not be retrieved")
            }
            
            outPtr.destroy(outBufferSize)
            errorPtr.destroy()
            
            return escapedString
        })
    }
    
    public var tableNames: [String] {
        let resultSet: ResultSet
        do {
            resultSet = try select("select tablename from pg_tables where schemaname = $1", params: ["public"])
        } catch Error.QueryError(let msg, _) {
            print(msg)
            return []
        } catch {
            print("Unknown error querying for tables")
            return []
        }

        let names = resultSet.rows().map({ row in return row["tablename"] as? String })
        return names.compact()
    }
    
    public func getTable(name: String) throws -> Table {
        throw UnimplementedError.Unimplemented
    }
}