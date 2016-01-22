//
//  PostgreSQLAdapterTests.swift
//  PostgreSQLAdapterTests
//
//  Created by Nat Budin on 1/9/16.
//  Copyright © 2016 Nat Budin. All rights reserved.
//

import XCTest
@testable import PostgreSQLAdapter

class PostgreSQLAdapterTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testExample() {
        do {
            let adapter = try PostgreSQLAdapter()
            let resultSet = try adapter.select("select * from pg_user")
            print("\(resultSet.rowCount) rows")
            print("Columns: \(resultSet.columnNames)")
            
            for row in resultSet.rows() {
                for columnName in resultSet.columnNames {
                    print("\(columnName): \(row[columnName])")
                }
            }
        } catch PostgreSQLAdapter.Error.ConnectionError(let msg) {
            XCTFail(msg)
        } catch {
            XCTFail("Unhandled error")
        }
    }
    
}
