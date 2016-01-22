//
//  ResultSet.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/10/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public protocol ResultSet {
    var rowCount: Int { get }
    var columnNames: [String] { get }
    
    func rows() -> AnyGenerator<ResultRow>
}