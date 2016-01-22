//
//  Table.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/9/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public protocol Table {
    var columnNames: [String] { get }
    func getColumn(name: String) throws -> Column
}