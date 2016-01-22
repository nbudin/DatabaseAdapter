//
//  StringArrayUtils.swift
//  DatabaseAdapter
//
//  Created by Nat Budin on 1/15/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import Foundation

typealias CString = UnsafePointer<CChar>
typealias CStringArray = UnsafePointer<CString>

protocol CStringConvertible {
    func withCString<Result>(@noescape f: UnsafePointer<Int8> throws -> Result) rethrows -> Result
}

extension String: CStringConvertible {
}

extension Array where Element: CStringConvertible {
    private func withCStringArray<Result>(stringArray: ArraySlice<Element>, f: (CStringArray throws -> Result), cStrings: [CString]) throws -> Result {
        if stringArray.isEmpty {
            return try ContiguousArray(cStrings + [nil]).withUnsafeBufferPointer({
                return try f(UnsafePointer<UnsafePointer<Int8>>($0.baseAddress))
            })
        } else {
            return try stringArray[stringArray.startIndex].withCString({ cString in
                let remainingStrings = stringArray.suffixFrom(stringArray.startIndex + 1)
                return try withCStringArray(remainingStrings, f: f, cStrings: cStrings + [cString])
            })
        }
    }
    
    func withCStringArray<Result>(f: (CStringArray throws -> Result)) throws -> Result {
        return try withCStringArray(self.suffixFrom(0), f: f, cStrings: [])
    }
}

extension String {
    @warn_unused_result
    public static func fromCString(cs: UnsafePointer<CChar>, encoding: NSStringEncoding, defaultValue: String) -> String {
        if let string = String(CString: cs, encoding: encoding) {
            return string
        } else {
            return defaultValue
        }
    }
}