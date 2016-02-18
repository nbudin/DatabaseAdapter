//
//  PostgreSQLType.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 2/17/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public protocol PostgreSQLType {
    func castToPostgreSQLString(obj: PostgreSQLCastable) -> String
}

public class PostgreSQLScalarType: PostgreSQLType {
    let destinationType: PostgreSQLCastableScalar.Type
    
    init(destinationType: PostgreSQLCastableScalar.Type) {
        self.destinationType = destinationType
    }
    
    public func castFromPostgreSQLString(string: String) -> PostgreSQLCastable? {
        return destinationType.castFromPostgreSQLString(string)
    }
    
    public func castToPostgreSQLString(obj: PostgreSQLCastable) -> String {
        return obj.castToPostgreSQLString()
    }
}

public class PostgreSQLArrayType: PostgreSQLType {
    let elementType: PostgreSQLCastableScalar.Type
    
    init(elementType: PostgreSQLCastableScalar.Type) {
        self.elementType = elementType
    }
    
    public func castFromPostgreSQLString(string: String) -> PostgreSQLArray? {
        return PostgreSQLArray(string: string, elementType: elementType)
    }
    
    public func castToPostgreSQLString(obj: PostgreSQLCastable) -> String {
        return obj.castToPostgreSQLString()
    }
}