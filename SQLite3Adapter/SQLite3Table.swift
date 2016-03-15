//
//  SQLite3Table.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 3/15/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation
import DatabaseAdapter

class SQLite3Table: Table {
    let name: String
    let adapter: SQLite3Adapter
    
    var columnNames: [String] {
        do {
            let resultSet = try adapter.select("PRAGMA table_info('\(name)');")
            return resultSet.rows().map({ row in return row["name"] as! String })
        } catch {
            return []
        }
    }
    
    init(adapter: SQLite3Adapter, name: String) {
        self.adapter = adapter
        self.name = name
    }
    
    func getColumn(name: String) throws -> Column {
        return SQLite3Column()
    }
}