//
//  AbstractAdapter.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/9/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public protocol DatabaseAdapter {
    var tableNames: [String] { get }
    func getTable(name: String) throws -> Table
    
    func execute(sql: String) throws -> Int
    func select(sql: String) throws -> ResultSet
}