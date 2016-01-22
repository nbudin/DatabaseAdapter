//
//  SequenceType+compact.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/15/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

protocol OptionalParasite {
    typealias WrappedParasite
    
    func toArray() -> [WrappedParasite]
}

extension Optional: OptionalParasite {
    typealias WrappedParasite = Wrapped
    
    func toArray() -> [WrappedParasite] {
        return flatMap { [$0] } ?? []
    }
}

extension ImplicitlyUnwrappedOptional: OptionalParasite {
    typealias WrappedParasite = Wrapped
    
    func toArray() -> [WrappedParasite] {
        return flatMap { [$0] } ?? []
    }
}


extension SequenceType where Generator.Element: OptionalParasite {
    func compact() -> [Generator.Element.WrappedParasite] {
        return flatMap { element in
            return element.toArray()
        }
    }
}