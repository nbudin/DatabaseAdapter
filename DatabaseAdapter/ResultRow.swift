//
//  ResultRow.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/10/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public struct ResultRow: SequenceType {
    public let columnNames: [String]
    let rowValues: [String : Any?]
    
    public init(columnNames: [String], columnValues: [Any?]) {
        var rowValues = [String : Any?]()
        
        for i in 0..<columnNames.count {
            rowValues[columnNames[i]] = columnValues[i]
        }
        
        self.columnNames = columnNames
        self.rowValues = rowValues
    }
    
    public subscript(columnName: String) -> Any? {
        guard let value = rowValues[columnName] else {
            return nil
        }
        
        return value
    }
    
    public subscript(columnIndex: Int) -> Any? {
        return self[columnNames[columnIndex]]
    }
    
    public func generate() -> AnyGenerator<Any?> {
        var columnNameGenerator = columnNames.generate()
        
        return anyGenerator({
            if let columnName = columnNameGenerator.next() {
                return self[columnName]
            } else {
                return nil
            }
        })
    }
}