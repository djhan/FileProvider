//
//  NSLockExtension.swift
//  ExtendOperation
//
//  Created by DJ.HAN on 2020/06/11.
//  Copyright © 2020 DJ.HAN. All rights reserved.
//

/**
 String Extension
 
 # 참고사항:
 
 - 원래 EdgeView 3의 NSLock Extension에 선언된 메쏘드
 - Common Library로 이동시켜 공통으로 사용하게끔 만드는 방법도 고려했으나, Common Library와 FilesProvider의 deployment 타겟이 다른 관계로 중지
 - Common Library의 deployment 타겟을 변경시키는 방법도 고려했으나, 향후 개발 방향을 생각하여 중지
 - 따라서 여기에 해당 메쏘드를 그대로 추가하기로 결정
*/
 
import Foundation

public extension NSLock {
    
    /// 동기화 락
    func withCriticalScope<T>(_ block: () -> T) -> T {
        lock()
        let value = block()
        unlock()
        return value
    }
}
