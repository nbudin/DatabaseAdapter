//
//  SQLite3ResultSet.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 2/18/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import DatabaseAdapter
import libsqlite3

class SQLite3ResultSet: ResultSet {
    let adapter: SQLite3Adapter
    let preparedStatement: SQLite3PreparedStatement
    
    var columnNames: [String] { return preparedStatement.columnNames }
    
    init(preparedStatement: SQLite3PreparedStatement) {
        self.preparedStatement = preparedStatement
        self.adapter = preparedStatement.adapter
    }
    
    func rows() -> AnyGenerator<ResultRow> {
        let columnNames = self.columnNames
        
        return anyGenerator({ () -> ResultRow? in
            let result = self.preparedStatement.step()
            
            switch (result) {
            case SQLITE_DONE:
                return nil
            case SQLITE_ROW:
                return self.getRow(columnNames)
            case SQLITE_BUSY:
                return nil // TODO: figure out wtf to do about this
            default:
                return nil // TODO: figure out how to handle errors
            }
        })
    }
    
    func getRow(columnNames: [String]) -> ResultRow {
        let dataCount = preparedStatement.dataCount()
        let values = (Int32(0)..<dataCount).map({ (index: Int32) -> Any? in return self.preparedStatement.columnValue(index) })
        
        return ResultRow(columnNames: columnNames, columnValues: values)
    }
}