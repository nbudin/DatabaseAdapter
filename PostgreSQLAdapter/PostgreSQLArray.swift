//
//  PostgreSQLArray.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 2/17/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

public final class PostgreSQLArray: CollectionType {
    public let elementType: PostgreSQLCastableScalar.Type
    public let elements: [PostgreSQLCastableScalar?]
    
    private static let DOUBLE_QUOTE: Character = "\""
    private static let BACKSLASH: Character = "\\"
    private static let COMMA: Character = ","
    private static let BRACKET_OPEN: Character = "{"
    private static let BRACKET_CLOSE: Character = "}"
    
    public var startIndex: Int { return elements.startIndex }
    public var endIndex: Int { return elements.endIndex }
    public subscript(position: Int) -> PostgreSQLCastableScalar? { return elements[position] }
    
    required public init(elements: Array<PostgreSQLCastableScalar?>, elementType: PostgreSQLCastableScalar.Type) {
        self.elements = elements
        self.elementType = elementType
    }
    
    public convenience init(string: String, elementType: PostgreSQLCastableScalar.Type) {
        self.init(elements: PostgreSQLArray.parse(string, elementType: elementType), elementType: elementType)
    }
    
    public static func parse(string: String, elementType: PostgreSQLCastableScalar.Type) -> [PostgreSQLCastableScalar?] {
        var localIndex = string.startIndex
        var array = [PostgreSQLCastableScalar?]()
        
        while (localIndex < string.endIndex) {
            switch (string[localIndex]) {
            case BRACKET_OPEN:
                let result = parseArrayContents(string, index: localIndex.successor(), elementType: elementType)
                localIndex = result.endIndex
                array = result.contents
            case BRACKET_CLOSE:
                return array
            default:
                localIndex = localIndex.successor()
            }
        }
        
        return array
    }
    
    private static func parseArrayContents(string: String, index: String.Index, elementType: PostgreSQLCastableScalar.Type) -> (endIndex: String.Index, contents: [PostgreSQLCastableScalar?]) {
        
        var isEscaping  = false
        var isQuoted = false
        var wasQuoted = false
        var currentItem = ""
        var localIndex = index
        var array = [PostgreSQLCastableScalar?]()
        
        while localIndex != string.endIndex {
            let token = string[localIndex]
            
            if isEscaping {
                currentItem.append(token)
                isEscaping = false
            } else {
                if isQuoted {
                    switch (token) {
                    case DOUBLE_QUOTE:
                        isQuoted = false
                        wasQuoted = false
                    case BACKSLASH:
                        isEscaping = true
                    default:
                        currentItem.append(token)
                    }
                } else {
                    switch token {
                    case BACKSLASH:
                        isEscaping = true
                    case COMMA:
                        addItemToArray(&array, currentItem: currentItem, quoted: wasQuoted, elementType: elementType)
                        currentItem = ""
                        wasQuoted = false
                    case DOUBLE_QUOTE:
                        isQuoted = true
                    case BRACKET_OPEN:
                        var internalItems = [PostgreSQLCastableScalar?]()
                        let result = parseArrayContents(string, index: localIndex.successor(), elementType: elementType)
                        localIndex = result.endIndex
                        internalItems = result.contents
                        // array.append(internalItems)
                        // TODO: figure out how to support multidimensional arrays
                    case BRACKET_CLOSE:
                        addItemToArray(&array, currentItem: currentItem, quoted: wasQuoted, elementType: elementType)
                        return (localIndex, array)
                    default:
                        currentItem.append(token)
                    }
                }
            }
            
            localIndex = localIndex.successor()
        }
        
        return (localIndex, array)
    }
    
    private static func addItemToArray(inout array: [PostgreSQLCastableScalar?], currentItem: String, quoted: Bool, elementType: PostgreSQLCastableScalar.Type) {
        if !quoted && currentItem.characters.count == 0 {
            return
        }
        
        if !quoted && currentItem == "NULL" {
            array.append(nil)
        } else {
            array.append(elementType.castFromPostgreSQLString(currentItem))
        }
    }
    
    public func castToPostgreSQLString() -> String {
        let elementStrings = elements.map({ element -> String in
            guard let element = element else {
                return "NULL"
            }
            
            return element.castToPostgreSQLString()
        })
        return "{\(elementStrings.joinWithSeparator(","))}"
    }
    
    public func generate() -> IndexingGenerator<[PostgreSQLCastableScalar?]> {
        return elements.generate()
    }
}