//
//  main.swift
//  DatabaseAdapterTestApp
//
//  Created by Nat Budin on 1/12/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import PostgreSQLAdapter
import SQLite3Adapter

//let adapter = try PostgreSQLAdapter(paramsDict: [
//    "host": "localhost",
//    "user": "postgres",
//    "dbname": "plm_schooner"
//])

let adapter = try SQLite3Adapter(filename: "/Users/nbudin/src/intercode/db/test.sqlite3")

print("Tables: \(adapter.tableNames)")

//let resultSet = try adapter.select("select id, hidden_user_ids from user_preferences where hidden_user_ids != '{}'::integer[] limit 1")
//print("\(resultSet.rowCount) rows")
//print("Columns: \(resultSet.columnNames)")
//
//
//for row in resultSet.rows() {
//    for columnName in row.columnNames {
//        let description: String
//        if let value = row[columnName] {
//            switch (value) {
//            case is PostgreSQLArray:
//                description = (value as! PostgreSQLArray).castToPostgreSQLString()
//            default:
//                description = value.description
//            }
//        } else {
//            description = "nil"
//        }
//        
//        print("\(columnName): \(description)")
//    }
//}