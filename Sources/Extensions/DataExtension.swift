//
//  DataExtension.swift
//  FilesProvider
//
//  Created by DJ.HAN on 2021/09/30.
//

import Foundation
import Cocoa

/**
 Data Extension
 
 # 참고사항:
 
 - Common Library를 임포트해서 공통으로 사용하게끔 만드는 방법도 고려했으나, Common Library와 FilesProvider의 deployment 타겟이 다른 관계로 중지
 - Common Library의 deployment 타겟을 변경시키는 방법도 고려했으나, 향후 개발 방향을 생각하여 중지
 - 따라서 여기에 해당 메쏘드를 약간 변경된 형태로 추가하기로 결정
 */
extension Data {
    /**
     String으로 추정되는 데이터의 인코딩을 추측, 스트링을 반환
     - Returns: 디코딩된 String 반환. 스트링이 아니거나, 추정 실패시 nil 반환
     */
    func autoDetectEncodingString() -> String? {
        // 자동 인코딩인 경우, 현재 사용 언어에 따른 캐릭터 셋을 미리 지정
        let preferredLanguage = Locale.preferredLanguages.first!
        let separates = preferredLanguage.components(separatedBy: "-")
        let languageCode = separates.first!

        // 추정 NSString 패스
        var detectedString: NSString?
        // 인코딩 추측으로 path 지정
        // likelyLanguageKey 옵션에 현재 사용 언어의 우선권을 부여한다
        _ = NSString.stringEncoding(for: self,
                                    encodingOptions: [.likelyLanguageKey: languageCode],
                                    convertedString: &detectedString,
                                    usedLossyConversion: nil)
        guard let convertedString = detectedString else { return nil }
        return convertedString as String
    }
    
    /**
     현재 data의 추정 인코딩 방식을 반환
     */
    var stringEncoding: String.Encoding? {
        var nsString: NSString?
        guard case let rawValue = NSString.stringEncoding(for: self, encodingOptions: nil, convertedString: &nsString, usedLossyConversion: nil), rawValue != 0 else { return nil }
        return .init(rawValue: rawValue)
    }
}
