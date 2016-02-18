//
//  PostgreSQLTypeMap.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/23/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

typealias Oid = UInt32

class PostgreSQLTypeMap {
    enum TypeMapError: ErrorType {
        case NoAdapter
    }
    
    var typesByTypeName: [String: PostgreSQLCastableScalar.Type] = [String: PostgreSQLCastableScalar.Type]()
    var typesByOID: [Oid: PostgreSQLCastableScalar.Type] = [Oid: PostgreSQLCastableScalar.Type]()
    var arrayElementTypesByOID: [Oid: PostgreSQLCastableScalar.Type] = [Oid: PostgreSQLCastableScalar.Type]()
    var queriedOIDs = Set<Oid>()
    var adapter: PostgreSQLAdapter?
    
    var registeredTypeNames: [String] {
        return Array(typesByTypeName.keys)
    }
    
    func registerType(typeName: String, type: PostgreSQLCastableScalar.Type) throws {
        try registerTypes([typeName: type])
    }
    
    func registerTypes(types: [String: PostgreSQLCastableScalar.Type]) throws {
        types.forEach({ typeName, type in
            typesByTypeName[typeName] = type
        })

        try populateTypeOIDs(Array(types.keys))
    }
    
    func typeForOID(OID: Oid, queryIfNotFound: Bool = false) -> PostgreSQLCastableScalar.Type? {
        if let type = typesByOID[OID] {
            return type
        } else {
            if queryIfNotFound && !queriedOIDs.contains(OID) {
                queriedOIDs.insert(OID)
                do {
                    try populateTypeOIDs([OID])
                    return typesByOID[OID]
                } catch {
                    return nil
                }
            } else {
                return nil
            }
        }
    }
    
    func arrayElementTypeForOID(OID: Oid) -> PostgreSQLCastableScalar.Type? {
        return arrayElementTypesByOID[OID]
    }
    
    private func populateTypeOIDs(typeNames: [String]) throws {
        guard let adapter = self.adapter else {
            throw TypeMapError.NoAdapter
        }
        
        let typeNamesEscaped = try typeNames.map({ typeName in return try adapter.escapeString(typeName) })
        let typeNamesQuoted = typeNamesEscaped.map({ typeName in return "'\(typeName)'" })
        try populateTypeOIDs(whereClause: "t.typname IN (\(typeNamesQuoted.joinWithSeparator(",")))")
    }
    
    private func populateTypeOIDs(OIDs: [Oid]) throws {
        let OIDstrings = OIDs.map({ oid in String(oid) })
        try populateTypeOIDs(whereClause: "t.oid IN (\(OIDstrings.joinWithSeparator(",")))")
    }
    
    private func populateTypeOIDs(whereClause whereClause: String) throws {
        guard let adapter = self.adapter else {
            throw TypeMapError.NoAdapter
        }
        
        let result = try adapter.select("SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype, t.typarray FROM pg_type as t LEFT JOIN pg_range as r ON oid = rngtypid WHERE \(whereClause)") as! PostgreSQLResultSet
        result.skipCastingValues()
        
        for row in result.rows() {
            guard let oidString = row["oid"] as? String else {
                continue
            }
            
            guard let OID = UInt32(oidString) else {
                continue
            }
            
            queriedOIDs.insert(OID)
            
            guard let typname = row["typname"] as? String else {
                continue
            }
            
            guard let castableType = typesByTypeName[typname] else {
                continue
            }
            
            typesByOID[OID] = castableType
            
            if let typarray = row["typarray"] as? String {
                if let typarrayOid = Oid(typarray) {
                    if typarrayOid != 0 {
                        arrayElementTypesByOID[typarrayOid] = castableType
                    }
                }
            }
        }
    }
}