//
//  main.swift
//  DatabaseAdapterTestApp
//
//  Created by Nat Budin on 1/12/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import PostgreSQLAdapter

let adapter = try PostgreSQLAdapter(paramsDict: [
    "host": "localhost",
    "user": "postgres",
    "dbname": "plm_schooner"
])

print("Tables: \(adapter.tableNames)")

let resultSet = try adapter.select("select * from pg_user")
print("\(resultSet.rowCount) rows")
print("Columns: \(resultSet.columnNames)")


for row in resultSet.rows() {
    for columnName in row.columnNames {
        let description: String
        if let value = row[columnName] {
            description = value.description
        } else {
            description = "nil"
        }
        
        print("\(columnName): \(description)")
    }
}