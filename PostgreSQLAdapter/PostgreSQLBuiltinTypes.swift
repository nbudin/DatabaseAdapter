//
//  BuiltinTypes.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/25/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

extension Bool: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        if self {
            return "t"
        } else {
            return "f"
        }
    }
    
    public static func castFromPostgreSQLString(string: String) -> Bool? {
        switch string {
            case "t":
                return true
            case "f":
                return false
        default:
            return nil
        }
    }
}

extension String: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        return self
    }
    
    public static func castFromPostgreSQLString(string: String) -> String? {
        return string
    }
}

extension Int16: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        return String(self)
    }
    
    public static func castFromPostgreSQLString(string: String) -> Int16? {
        return Int16(string)
    }
}

extension Int32: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        return String(self)
    }
    
    public static func castFromPostgreSQLString(string: String) -> Int32? {
        return Int32(string)
    }
}

extension Int64: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        return String(self)
    }
    
    public static func castFromPostgreSQLString(string: String) -> Int64? {
        return Int64(string)
    }
}

extension UInt32: PostgreSQLCastableScalar {
    public func castToPostgreSQLString() -> String {
        return String(self)
    }
    
    public static func castFromPostgreSQLString(string: String) -> UInt32? {
        return UInt32(string)
    }
}

let PostgreSQLBuiltinTypes: Dictionary<String, PostgreSQLCastableScalar.Type> = [
    "bool": Bool.self,
    "text": String.self,
    "name": String.self,
    "int2": Int16.self,
    "int4": Int32.self,
    "int8": Int64.self,
    "oid": UInt32.self
]