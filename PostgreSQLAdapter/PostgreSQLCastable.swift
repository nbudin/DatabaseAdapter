//
//  PostgreSQLType.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/23/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public protocol PostgreSQLCastable {
    static func castFromPostgreSQLString(string: String) -> Self?
    func castToPostgreSQLString() -> String
}

public protocol PostgreSQLCastableScalar: PostgreSQLCastable {
    init()
}