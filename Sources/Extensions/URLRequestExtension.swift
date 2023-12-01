//
//  URLRequestExtension.swift
//  FilesProvider
//
//  Created by DJ.HAN on 12/1/23.
//

import Foundation
import Cocoa

extension URLRequest {

    /// Update request for HTTP authentication
    ///
    /// - parameter username:        The username
    /// - parameter password:        The password
    /// - parameter authentication:  The type of authentication to be applied

    mutating func updateBasicAuth(for username: String, password: String, authentication: String = kCFHTTPAuthenticationSchemeBasic as String) {
        let message = CFHTTPMessageCreateRequest(kCFAllocatorDefault, httpMethod! as CFString, url! as CFURL, kCFHTTPVersion1_1).takeRetainedValue()
        if !CFHTTPMessageAddAuthentication(message, nil, username as CFString, password as CFString, authentication as CFString?, false) {
            print("authentication not added")
        }
        if let authorizationString = CFHTTPMessageCopyHeaderFieldValue(message, "Authorization" as CFString)?.takeRetainedValue() {
            setValue(authorizationString as String, forHTTPHeaderField: "Authorization")
        } else {
            print("didn't find authentication header")
        }
    }
}
