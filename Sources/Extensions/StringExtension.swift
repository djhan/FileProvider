//
//  StringExtension.swift
//  FilesProvider
//
//  Created by DJ.HAN on 2021/04/30.
//

import Foundation
import Cocoa

/**
 String Extension
 
 # 참고사항:
 
 - 원래 EdgeView 3의 String Extension에 선언된 카테고리/작가 메쏘드를 포함하고 있다
 - Common Library로 이동시켜 공통으로 사용하게끔 만드는 방법도 고려했으나, Common Library와 FilesProvider의 deployment 타겟이 다른 관계로 중지
 - Common Library의 deployment 타겟을 변경시키는 방법도 고려했으나, 향후 개발 방향을 생각하여 중지
 - 따라서 여기에 해당 메쏘드를 약간 변경된 형태로 추가하기로 결정
 */
extension String {
    /// 카테고리 반환
    var category: String? {
        return self.getInformation(0)
    }
    /// 작가 반환
    var writer: String? {
        return self.getInformation(1)
    }
    /// 내부 정보를 추출해서 반환하는 private 메쏘드
    private func getInformation(_ information: Int) -> String? {
        // 정규 표현식으로 추출한다
        var regex: NSRegularExpression
        switch information {
        // 카테고리 추출시
        case 0: regex = try! NSRegularExpression(pattern: "(?<=\\().*?(?=\\))", options: .caseInsensitive)
            
        // 작가 추출시
        case 1: regex = try! NSRegularExpression(pattern: "(?<=\\[).*?(?=\\])", options: .caseInsensitive)
            
        // 그 외의 경우 - 중지
        default: return nil
        }
        // 최초에 일치하는 값을 가져온다
        // 없는 경우 nil 반환
        guard let resultFirstMatch = regex.firstMatch(in: self, options: [], range: NSRange(startIndex..., in: self)) else { return nil }
        return (self as NSString).substring(with: resultFirstMatch.range)
    }
}
