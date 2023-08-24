//
//  FTPFileProvider.swift
//  FileProvider
//
//  Created by Amir Abbas Mousavian.
//  Copyright © 2017 Mousavian. Distributed under MIT license.
//

import Foundation
import CommonLibrary

/**
 Allows accessing to FTP files and directories. This provider doesn't cache or save files internally.
 It's a complete reimplementation and doesn't use CFNetwork deprecated API.
 */
open class FTPFileProvider: NSObject, FileProviderBasicRemote, FileProviderOperations, FileProviderReadWrite, FileProviderReadWriteProgressive {
    
    /// FTP data connection mode.
    public enum Mode: String {
        /// Passive mode for FTP and Extended Passive mode for FTP over TLS.
        case `default`
        /// Data connection would establish by client to determined server host/port.
        case passive
        /// Data connection would establish by server to determined client's port.
        case active
        /// Data connection would establish by client to determined server host/port, with IPv6 support. (RFC 2428)
        case extendedPassive
    }
    
    open class var type: String { return "FTP" }
    public let baseURL: URL?
    
    /// 인코딩 방식
    public var encoding: String.Encoding = .utf8
    
    /// 에러 발생시 재시도 횟수
    //var tryErrorLimit: Int = 0
    
    open var dispatch_queue: DispatchQueue
    open var operation_queue: OperationQueue {
        willSet {
            assert(_session == nil, "It's not effective to change dispatch_queue property after session is initialized.")
        }
    }
    
    open weak var delegate: FileProviderDelegate?
    open var credential: URLCredential? {
        didSet {
            sessionDelegate?.credential = self.credential
        }
    }
    open private(set) var cache: URLCache?
    public var useCache: Bool
    public var validatingCache: Bool
    
    /// Determine either FTP session is in passive or active mode.
    public let mode: Mode
    /**
     내부 프로퍼티 동기화 처리를 위한 NSLock
     - Stack Overflow의 [동기화 관련 질답](https://stackoverflow.com/questions/43561169/trying-to-understand-asynchronous-operation-subclass) 참고
     */
    private let stateLock = NSLock()

    fileprivate var _session: URLSession!
    internal var sessionDelegate: SessionDelegate?
    public var session: URLSession {
        get {
            self.stateLock.withCriticalScope { [weak self] () -> URLSession in
                guard let strongSelf = self else {
                    print("FTPFileProvider>session: StrongSelf가 nil, 더미값 반환")
                    return URLSession(configuration: URLSessionConfiguration.default, delegate: self?.sessionDelegate as URLSessionDelegate?, delegateQueue: nil)
                }
                if strongSelf._session == nil {
                    strongSelf.sessionDelegate = SessionDelegate(fileProvider: strongSelf)
                    let config = URLSessionConfiguration.default
                    strongSelf._session = URLSession(configuration: config, delegate: strongSelf.sessionDelegate as URLSessionDelegate?, delegateQueue: strongSelf.operation_queue)
                    strongSelf._session?.sessionDescription = UUID().uuidString
                    /**
                     # Error 발생
                     - 2022/02/06
                     - Fatal Error: Unexpectedly found nil while unwrapping an Optional value
                     - 원인은 잘 모르겠지만 get 하는 도중, nil로 릴리즈되는 경우가 있는 게 아닌가 추측된다
                     */
                    initEmptySessionHandler(strongSelf._session!.sessionDescription!)
                }
                return strongSelf._session
            }
        }
        
        set {
            self.stateLock.withCriticalScope { [weak self] () -> Void in
                guard let strongSelf = self else { return }
                assert(newValue.delegate is SessionDelegate, "session instances should have a SessionDelegate instance as delegate.")
                strongSelf._session = newValue
                if strongSelf._session?.sessionDescription?.isEmpty ?? true {
                    strongSelf._session?.sessionDescription = UUID().uuidString
                }
                strongSelf.sessionDelegate = newValue.delegate as? SessionDelegate
                initEmptySessionHandler(strongSelf._session!.sessionDescription!)
            }
        }
    }
    
    #if os(macOS) || os(iOS) || os(tvOS)
    open var undoManager: UndoManager? = nil
    #endif
    
    /**
     Initializer for FTP provider with given username and password.
     
     - Note: `passive` value should be set according to server settings and firewall presence.
     
     - Parameter baseURL: a url with `ftp://hostaddress/` format.
     - Parameter mode: FTP server data connection type.
     - Parameter encoding: 인코딩 값
     - Parameter credential: a `URLCredential` object contains user and password.
     - Parameter cache: A URLCache to cache downloaded files and contents. (unimplemented for FTP and should be nil)
     
     - Important: Extended Passive or Active modes will fallback to normal Passive or Active modes if your server
         does not support extended modes.
     */
    public init? (baseURL: URL,
                  mode: Mode = .default,
                  encoding: String.Encoding,
                  credential: URLCredential? = nil,
                  cache: URLCache? = nil) {
        guard ["ftp", "ftps", "ftpes"].contains(baseURL.uw_scheme.lowercased()) else {
            return nil
        }
        // 에러 회피책
        guard baseURL.host != nil,
              var urlComponents = URLComponents(url: baseURL, resolvingAgainstBaseURL: true) else {
            if #available(macOS 11.0, *) {
                EdgeLogger.shared.networkLogger.log(level: .error, "URLComponents 초기화 실패.")
            }
            return nil
        }
        //var urlComponents = URLComponents(url: baseURL, resolvingAgainstBaseURL: true)!
        let defaultPort: Int = baseURL.scheme?.lowercased() == "ftps" ? 990 : 21
        urlComponents.port = urlComponents.port ?? defaultPort
        urlComponents.scheme = urlComponents.scheme ?? "ftp"
        urlComponents.path = urlComponents.path.hasSuffix("/") ? urlComponents.path : urlComponents.path + "/"
        
        self.baseURL =  urlComponents.url!.absoluteURL
        self.mode = mode
        self.useCache = false
        self.validatingCache = true
        self.cache = cache
        self.credential = credential
        self.supportsRFC3659 = true
        self.encoding = encoding
        
        let queueLabel = "FileProvider.\(Swift.type(of: self).type)"
        dispatch_queue = DispatchQueue(label: queueLabel, attributes: .concurrent)
        operation_queue = OperationQueue()
        operation_queue.name = "\(queueLabel).Operation"
        
        super.init()
    }
    
    /**
     **DEPRECATED** Initializer for FTP provider with given username and password.
     
     - Note: `passive` value should be set according to server settings and firewall presence.
     
     - Parameter baseURL: a url with `ftp://hostaddress/` format.
     - Parameter passive: FTP server data connection, `true` means passive connection (data connection created by client)
     and `false` means active connection (data connection created by server). Default is `true` (passive mode).
     - Parameter encoding: 인코딩 값
     - Parameter credential: a `URLCredential` object contains user and password.
     - Parameter cache: A URLCache to cache downloaded files and contents. (unimplemented for FTP and should be nil)
     */
    @available(*, deprecated, renamed: "init(baseURL:mode:credential:cache:)")
    public convenience init? (baseURL: URL,
                              passive: Bool,
                              encoding: String.Encoding,
                              credential: URLCredential? = nil,
                              cache: URLCache? = nil) {
        self.init(baseURL: baseURL, mode: passive ? .passive : .active, encoding: encoding, credential: credential, cache: cache)
    }
    
    public required convenience init?(coder aDecoder: NSCoder) {
        guard let baseURL = aDecoder.decodeObject(of: NSURL.self, forKey: "baseURL") as URL? else {
            if #available(macOS 10.11, iOS 9.0, tvOS 9.0, *) {
                aDecoder.failWithError(CocoaError(.coderValueNotFound,
                                                  userInfo: [NSLocalizedDescriptionKey: "Base URL is not set."]))
            }
            return nil
        }
        let mode: Mode
        if let modeStr = aDecoder.decodeObject(of: NSString.self, forKey: "mode") as String?, let mode_v = Mode(rawValue: modeStr) {
            mode = mode_v
        } else {
            let passiveMode = aDecoder.decodeBool(forKey: "passiveMode")
            mode = passiveMode ? .passive : .active
        }
        let encoding = String.Encoding.init(rawValue: UInt(aDecoder.decodeInteger(forKey: "encoding")))
        self.init(baseURL: baseURL, mode: mode, encoding: encoding, credential: aDecoder.decodeObject(of: URLCredential.self, forKey: "credential"))
        self.useCache              = aDecoder.decodeBool(forKey: "useCache")
        self.validatingCache       = aDecoder.decodeBool(forKey: "validatingCache")
        self.supportsRFC3659       = aDecoder.decodeBool(forKey: "supportsRFC3659")
        self.securedDataConnection = aDecoder.decodeBool(forKey: "securedDataConnection")
    }
    
    public func encode(with aCoder: NSCoder) {
        aCoder.encode(self.baseURL, forKey: "baseURL")
        aCoder.encode(self.credential, forKey: "credential")
        aCoder.encode(self.useCache, forKey: "useCache")
        aCoder.encode(self.validatingCache, forKey: "validatingCache")
        aCoder.encode(self.mode.rawValue, forKey: "mode")
        aCoder.encode(self.supportsRFC3659, forKey: "supportsRFC3659")
        aCoder.encode(self.securedDataConnection, forKey: "securedDataConnection")
        aCoder.encode(self.encoding.rawValue, forKey: "encoding")
    }
    
    public static var supportsSecureCoding: Bool {
        return true
    }
    
    open func copy(with zone: NSZone? = nil) -> Any {
        let copy = FTPFileProvider(baseURL: self.baseURL!, mode: self.mode, encoding: self.encoding, credential: self.credential, cache: self.cache)!
        copy.delegate = self.delegate
        copy.fileOperationDelegate = self.fileOperationDelegate
        copy.useCache = self.useCache
        copy.validatingCache = self.validatingCache
        copy.securedDataConnection = self.securedDataConnection
        copy.supportsRFC3659 = self.supportsRFC3659
        return copy
    }
    
    deinit {
        if let sessionuuid = _session?.sessionDescription {
            removeSessionHandler(for: sessionuuid)
        }
        
        if fileProviderCancelTasksOnInvalidating {
            _session?.invalidateAndCancel()
        } else {
            _session?.finishTasksAndInvalidate()
        }
    }
    
    internal var supportsRFC3659: Bool
    
    /**
     Uploads files in chunk if `true`, Otherwise It will uploads entire file/data as single stream.
     
     - Note: Due to an internal bug in `NSURLSessionStreamTask`, it must be `true` when using Apple's stream task,
         otherwise it will occasionally throw `Assertion failed: (_writeBufferAlreadyWrittenForNextWrite == 0)`
         fatal error. My implementation of `FileProviderStreamTask` doesn't have this bug.
     
     - Note: Disabling this option will increase upload speed.
    */
    public var uploadByREST: Bool = false
    
    /**
     Determines data connection must TLS or not. `false` value indicates to use `PROT C` and
     `true` value indicates to use `PROT P`. Default is `true`.
    */
    public var securedDataConnection: Bool = true
    
    /**
     Trust all certificates if `disableEvaluation`, Otherwise validate certificate chain.
     Default is `performDefaultEvaluation`.
     */
    public var serverTrustPolicy: ServerTrustPolicy = .performDefaultEvaluation(validateHost: true)
    
    /**
     디렉토리 목록 생성
     */
    open func contentsOfDirectory(path: String, completionHandler: @escaping ([FileObject], Error?) -> Void) {
        self.contentsOfDirectory(path: path, rfc3659enabled: supportsRFC3659, completionHandler: completionHandler)
    }
    /**
     디렉토리 목록 생성
     - Returns: Progress 반환
     */
    open func contentsOfDirectoryWithProgress(path: String, completionHandler: @escaping ([FileObject], Error?) -> Void) -> Progress? {
        return self.contentsOfDirectory(path: path, rfc3659enabled: supportsRFC3659, completionHandler: completionHandler)
    }

    /**
     Returns an Array of `FileObject`s identifying the the directory entries via asynchronous completion handler.
     
     If the directory contains no entries or an error is occured, this method will return the empty array.

     - Progress를 반환하도록 수정 처리

     - Parameters:
        - path: path to target directory. If empty, root will be iterated.
        - rfc3659enabled: uses MLST command instead of old LIST to get files attributes, default is `true`.
        - tryCount: 재시도 횟수. 기본값은 nil로 최초 실행될 때 0으로 세팅된다. 이후 재귀적으로 실행시 1씩 증가된 값이 지정된다
        - completionHandler: a closure with result of directory entries or error.
        - contents: An array of `FileObject` identifying the the directory entries.
        - error: Error returned by system.
     */
    @discardableResult
    open func contentsOfDirectory(path apath: String,
                                  rfc3659enabled: Bool,
                                  tryCount: Int? = nil,
                                  completionHandler: @escaping (_ contents: [FileObject], _ error: Error?) -> Void) -> Progress? {
        let path = ftpPath(apath)
        
        // 재시도 횟수
        // tryCount가 nil이 아닌 경우, 그 값을 사용. nil인 경우 0으로 지정
        let tryCount = tryCount != nil ? tryCount! : 0
        
        let operation = FileOperationType.fetch(path: apath)
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true,
              let host = self.baseURL?.host,
              let port = self.baseURL?.port else {
            // 에러 처리후 nil 반환
            let error = FileProviderFTPError.unknownError(atPath: apath)
            completionHandler([], error)
            self.delegateNotify(operation, error: error)
            return nil
        }
        // 최종 갯수 1개의 progress 생성
        let progress = Progress(totalUnitCount: 1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)

        let task = session.fpstreamTask(withHostName: host, port: port)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = FileOperationType.fetch(path: path).json
        progress.cancellationHandler = {
            task.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                completionHandler([], FileProviderFTPError.unknownError(atPath: apath))
                self?.delegateNotify(operation, error: error)
                return
            }
            if let error = error {
                strongSelf.dispatch_queue.async {  [weak self] in
                    completionHandler([], error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            progress.setUserInfoObject(Date(), forKey: .startingTimeKey)

            strongSelf.ftpList(task, of: strongSelf.ftpPath(path), useMLST: rfc3659enabled, completionHandler: { [weak self] (contents, error) in
                guard let strongSelf = self else {
                    completionHandler([], FileProviderFTPError.unknownError(atPath: apath))
                    self?.delegateNotify(operation, error: error)
                    return
                }

                defer {
                    // 완료 갯수 1 증가
                    progress.completedUnitCount += 1
                    strongSelf.delegateNotify(operation, progress: progress.fractionCompleted)
                    strongSelf.ftpQuit(task)
                }
                // 에러 발생시
                if let error = error {
                    //---------------------------------------------------------------//
                    /// 에러 처리 내부 메쏘드
                    func processError() {
                        strongSelf.dispatch_queue.async {
                            completionHandler([], error)
                            strongSelf.delegateNotify(operation, error: error)
                        }
                    }
                    //---------------------------------------------------------------//
                    if let uerror = error as? URLError {
                        // unsupportedURL / timedOut에러인 경우
                        // rfc3659enabled 를 false 로 해서 재작업 진행
                        if uerror.code == .unsupportedURL ||
                            uerror.code == .timedOut {
                            
                            // 에러 재시도 횟수가 5회를 넘었는지 확인
                            guard tryCount < 5 else {
                                // 5회째인 경우, 에러 처리후 종료
                                return processError()
                            }

                            // Progress 전체 개수 증가
                            progress.totalUnitCount += 1
                            
                            // 추가 Progresss
                            let appendedProgress = strongSelf.contentsOfDirectory(path: path,
                                                                                  rfc3659enabled: false,
                                                                                  tryCount: tryCount + 1,
                                                                                  completionHandler: completionHandler)
                            // 현재 progress에 하위 Progress로 추가
                            if appendedProgress != nil {
                                progress.addChild(appendedProgress!, withPendingUnitCount: 1)
                                // 종료 처리
                                return
                            }
                            //strongSelf.delegateNotify(operation, error: error)
                            //return
                        }
                    }
                    
                    // 그 외의 경우 에러로 종료 처리
                    processError()
                    return
                }
                
                let files: [FileObject] = contents.compactMap {
                    rfc3659enabled ? strongSelf.parseMLST($0, in: path) : (strongSelf.parseUnixList($0, in: path) ?? strongSelf.parseDOSList($0, in: path))
                }
                
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler(files, nil)
                    self?.delegateNotify(operation, error: nil)
                }
            })
        }
        
        return progress
    }
    
    open func attributesOfItem(path: String, completionHandler: @escaping (FileObject?, Error?) -> Void) {
        self.attributesOfItem(path: path, rfc3659enabled: supportsRFC3659, completionHandler: completionHandler)
    }
    
    /**
     Returns a `FileObject` containing the attributes of the item (file, directory, symlink, etc.) at the path in question via asynchronous completion handler.
     
     If the directory contains no entries or an error is occured, this method will return the empty `FileObject`.
     
     - Parameter path: path to target directory. If empty, attributes of root will be returned.
     - Parameter rfc3659enabled: uses MLST command instead of old LIST to get files attributes, default is true.
     - Parameter completionHandler: a closure with result of directory entries or error.
         `attributes`: A `FileObject` containing the attributes of the item.
         `error`: Error returned by system.
     */
    open func attributesOfItem(path apath: String, rfc3659enabled: Bool, completionHandler: @escaping (_ attributes: FileObject?, _ error: Error?) -> Void) {
        let path = ftpPath(apath)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = FileOperationType.fetch(path: path).json
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    return completionHandler(nil, FileProviderFTPError.unknownError(atPath: path))
                }
                return
            }
            if let error = error {
                strongSelf.dispatch_queue.async {
                    completionHandler(nil, error)
                }
                return
            }
            
            let command = rfc3659enabled ? "MLST \(path)" : "LIST \(path)"
            strongSelf.execute(command: command, on: task, completionHandler: { [weak self] (response, error) in
                defer {
                    self?.ftpQuit(task)
                }
                
                guard let strongSelf = self else {
                    self?.dispatch_queue.async {
                        return completionHandler(nil, FileProviderFTPError.unknownError(atPath: path))
                    }
                    return
                }
                
                do {
                    if let error = error {
                        throw error
                    }
                    
                    guard let response = response, response.hasPrefix("250") || (response.hasPrefix("50") && rfc3659enabled) else {
                        throw URLError(.badServerResponse, url: strongSelf.url(of: path))
                    }
                    
                    if response.hasPrefix("500") {
                        strongSelf.supportsRFC3659 = false
                        strongSelf.attributesOfItem(path: path, rfc3659enabled: false, completionHandler: completionHandler)
                    }
                    
                    let lines = response.components(separatedBy: "\n").compactMap { $0.isEmpty ? nil : $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    guard lines.count > 2 else {
                        throw URLError(.badServerResponse, url: strongSelf.url(of: path))
                    }
                    let dirPath = path.deletingLastPathComponent
                    let file: FileObject? = rfc3659enabled ?
                        strongSelf.parseMLST(lines[1], in: dirPath) :
                        (strongSelf.parseUnixList(lines[1], in: dirPath) ?? strongSelf.parseDOSList(lines[1], in: dirPath))
                    strongSelf.dispatch_queue.async {
                        completionHandler(file, nil)
                    }
                } catch {
                    strongSelf.dispatch_queue.async {
                        completionHandler(nil, error)
                    }
                }
            })
        }
    }
    
    open func storageProperties(completionHandler: @escaping (_ volume: VolumeObject?) -> Void) {
        dispatch_queue.async {
            completionHandler(nil)
        }
    }
    
    /**
     FTP Provider의 확장 검색 메쏘드
     - 기본 메쏘드에서 일부 변경 처리. 실제로는 이 메쏘드를 사용하도록 한다
     - Parameters:
        - path: 검색 대상 경로
        - recursive: 재귀적 검색 여부
        - query: predicate 쿼리
        - tryCount: 재시도 횟수. 기본값은 nil로 최초 실행될 때 0으로 세팅된다. 이후 재귀적으로 실행시 1씩 증가된 값이 지정된다
        - foundItemsHandler: 중간 완료 핸들러. 현재 발견된 아이템 배열 반환
        - completionHandler: 최종 완료 핸들러. 모든 발견된 아이템 배열 반환
        - finalFiles: 최종 발견된 아이템 배열
        - error: 에러값
     - Returns: Progress. 옵셔널
     */
    @discardableResult
    open func searchFiles(path: String,
                          recursive: Bool,
                          query: NSPredicate,
                          tryCount: Int? = nil,
                          foundItemsHandler: ((_ checkFiles: [FileObject]) -> Void)?,
                          completionHandler: @escaping (_ finalFiles: [FileObject], _ error: Error?) -> Void) -> Progress? {
        // 반환할 progress
        var progress: Progress?
        
        // 재시도 횟수
        // tryCount가 nil이 아닌 경우, 그 값을 사용. nil인 경우 0으로 지정
        let tryCount = tryCount != nil ? tryCount! : 0

        //---------------------------------------------------------------------------------------------------//
        /// 에러 발생시 재시도 내부 메쏘드
        /// - Returns: 재시도시 true 반환
        func checkRetry(_ error: Error) -> Bool {
            // URLError인 경우
            if let uerror = error as? URLError {
                // unsupportedURL / timedOut 에러인 경우
                // rfc3659enabled 를 false 로 해서 재작업 진행
                if uerror.code == .unsupportedURL ||
                    uerror.code == .timedOut {
                    
                    // 에러 재시도 횟수가 5회를 넘었는지 확인
                    guard tryCount < 5 else {
                        // 5회째인 경우, 에러 처리후 종료
                        completionHandler([], error)
                        return false
                    }

                    // Progress 전체 개수 증가
                    progress?.totalUnitCount += 1

                    // 재시도
                    let appendedProgress = self.searchFiles(path: path,
                                                            recursive: recursive,
                                                            query: query,
                                                            tryCount: tryCount + 1,
                                                            foundItemsHandler: foundItemsHandler,
                                                            completionHandler: completionHandler)
                    if appendedProgress != nil {
                        progress?.addChild(appendedProgress!, withPendingUnitCount: 1)
                        return true
                    }
                }
            }
            // 그 외의 경우
            return false
        }
        //---------------------------------------------------------------------------------------------------//

        // 재귀적 검색 사용시
        if recursive {
            progress = self.recursiveList(path: path, useMLST: true, foundItemsHandler: { items in
                if let foundItemsHandler = foundItemsHandler {
                    // 중간 완료 핸들러 반환
                    let foundFiles = items.filter { query.evaluate(with: $0.mapPredicate()) }
                    foundItemsHandler(foundFiles)
                }
            }, completionHandler: { files, error in
                // 에러 발생시
                if let error = error {
                    // 재시도 여부 확인
                    if checkRetry(error) == true { return }
                    
                    // 재시도 실패시
                    // 에러 처리후 종료
                    completionHandler([], error)
                    return
                }

                // 최종 완료 핸들러 반환
                let foundFiles = files.filter { query.evaluate(with: $0.mapPredicate()) }
                completionHandler(foundFiles, nil)
            })
        } else {
            progress = self.contentsOfDirectoryWithProgress(path: path, completionHandler: { (items, error) in
                // 에러 발생시
                if let error = error {
                    // 재시도 여부 확인
                    if checkRetry(error) == true { return }
                    
                    // 재시도 실패시
                    // 에러 처리후 종료
                    completionHandler([], error)
                    return
                }
                
                let foundFiles = items.filter { query.evaluate(with: $0.mapPredicate()) }
                // 최종 완료 핸들러 반환
                completionHandler(foundFiles, nil)
            })
        }
        return progress
    }

    /**
     FTP Provider의 기본 검색 메쏘드
     */
    @discardableResult
    open func searchFiles(path: String,
                          recursive: Bool,
                          query: NSPredicate,
                          foundItemHandler: ((FileObject) -> Void)?,
                          completionHandler: @escaping (_ files: [FileObject], _ error: Error?) -> Void) -> Progress? {
        guard recursive == true else {
            // 재귀적 검색이 불필요한 경우
            return self.contentsOfDirectoryWithProgress(path: path, completionHandler: { (items, error) in
                if let error = error {
                    completionHandler([], error)
                    return
                }
                
                var result = [FileObject]()
                for item in items where query.evaluate(with: item.mapPredicate()) {
                    foundItemHandler?(item)
                    result.append(item)
                }
                completionHandler(result, nil)
            })
        }
        
        //재귀적 검색 실행시
        return self.recursiveList(path: path, useMLST: true, foundItemsHandler: { items in
            if let foundItemHandler = foundItemHandler {
                for item in items where query.evaluate(with: item.mapPredicate()) {
                    foundItemHandler(item)
                }
            }
        }, completionHandler: { files, error in
            if let error = error {
                completionHandler([], error)
                return
            }
            
            let foundFiles = files.filter { query.evaluate(with: $0.mapPredicate()) }
            completionHandler(foundFiles, nil)
        })
    }
    
    open func url(of path: String?) -> URL {
        let path = path?.trimmingCharacters(in: CharacterSet(charactersIn: "/ ")).addingPercentEncoding(withAllowedCharacters: .filePathAllowed) ?? (path ?? "")
        
        var baseUrlComponent = URLComponents(url: self.baseURL!, resolvingAgainstBaseURL: true)
        baseUrlComponent?.user = credential?.user
        baseUrlComponent?.password = credential?.password
        return URL(string: path, relativeTo: baseUrlComponent?.url ?? baseURL) ?? baseUrlComponent?.url ?? baseURL!
    }
    
    open func relativePathOf(url: URL) -> String {
        // check if url derieved from current base url
        let relativePath = url.relativePath
        if !relativePath.isEmpty, url.baseURL == self.baseURL {
            return (relativePath.removingPercentEncoding ?? relativePath).replacingOccurrences(of: "/", with: "", options: .anchored)
        }
        
        if !relativePath.isEmpty, self.baseURL == self.url(of: "/") {
            return (relativePath.removingPercentEncoding ?? relativePath).replacingOccurrences(of: "/", with: "", options: .anchored)
        }
        
        return relativePath.replacingOccurrences(of: "/", with: "", options: .anchored)
    }
    
    open func isReachable(completionHandler: @escaping (_ success: Bool, _ error: Error?) -> Void) {
        self.attributesOfItem(path: "/") { (file, error) in
            completionHandler(file != nil, error)
        }
    }
    
    open weak var fileOperationDelegate: FileOperationDelegate?
    
    @discardableResult
    open func create(folder folderName: String, at atPath: String, completionHandler: SimpleCompletionHandler) -> Progress? {
        let path = atPath.appendingPathComponent(folderName) + "/"
        return doOperation(.create(path: path), completionHandler: completionHandler)
    }
    
    @discardableResult
    open func moveItem(path: String, to toPath: String, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        return doOperation(.move(source: path, destination: toPath), completionHandler: completionHandler)
    }
    
    @discardableResult
    open func copyItem(path: String, to toPath: String, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        return doOperation(.copy(source: path, destination: toPath), completionHandler: completionHandler)
    }
    
    @discardableResult
    open func removeItem(path: String, completionHandler: SimpleCompletionHandler) -> Progress? {
        return doOperation(.remove(path: path), completionHandler: completionHandler)
    }
    
    @discardableResult
    open func copyItem(localFile: URL, to toPath: String, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        guard (try? localFile.checkResourceIsReachable()) ?? false else {
            dispatch_queue.async {
                completionHandler?(URLError(.fileDoesNotExist, url: localFile))
            }
            return nil
        }
        
        // check file is not a folder
        guard (try? localFile.resourceValues(forKeys: [.fileResourceTypeKey]))?.fileResourceType ?? .unknown == .regular else {
            dispatch_queue.async {
                completionHandler?(URLError(.fileIsDirectory, url: localFile))
            }
            return nil
        }
        
        let operation = FileOperationType.copy(source: localFile.absoluteString, destination: toPath)
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    let error = FileProviderFTPError.unknownError(atPath: toPath)
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            if let error = error {
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            
            guard let stream = InputStream(url: localFile) else {
                return
            }
            let size = localFile.fileSize
            strongSelf.ftpStore(task, filePath: strongSelf.ftpPath(toPath), from: stream, size: size, onTask: { task in
                weak var weakTask = task
                progress.cancellationHandler = {
                    weakTask?.cancel()
                }
                progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
            }, onProgress: { bytesSent, totalSent, expectedBytes in
                progress.totalUnitCount = expectedBytes
                progress.completedUnitCount = totalSent
                strongSelf.delegateNotify(operation, progress: progress.fractionCompleted)
            }, completionHandler: { [weak self] (error) in
                guard let strongSelf = self else {
                    self?.dispatch_queue.async {
                        let error = FileProviderFTPError.unknownError(atPath: toPath)
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }

                if error != nil {
                    progress.cancel()
                }
                strongSelf.ftpQuit(task)
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
            })
        }
        
        return progress
    }
    
    @discardableResult
    open func copyItem(path: String, toLocalURL destURL: URL, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.copy(source: path, destination: destURL.absoluteString)
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    let error = FileProviderFTPError.unknownError(atPath: path)
                    completionHandler?(error)
                }
                return
            }

            if let error = error {
                strongSelf.dispatch_queue.async {
                    completionHandler?(error)
                }
                return
            }
            
            let tempURL = URL(fileURLWithPath: NSTemporaryDirectory().appendingPathComponent(UUID().uuidString))
            guard let stream = OutputStream(url: tempURL, append: false) else {
                completionHandler?(CocoaError(.fileWriteUnknown, path: destURL.path))
                return
            }
            strongSelf.ftpDownload(task, filePath: strongSelf.ftpPath(path), to: stream, onTask: { task in
                weak var weakTask = task
                progress.cancellationHandler = {
                    weakTask?.cancel()
                }
                progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
            }, onProgress: { recevied, totalReceived, totalSize in
                progress.totalUnitCount = totalSize
                progress.completedUnitCount = totalReceived
                strongSelf.delegateNotify(operation, progress: progress.fractionCompleted)
            }) { [weak self] (error) in
                guard let strongSelf = self else {
                    self?.dispatch_queue.async { [weak self] in
                        let error = FileProviderFTPError.unknownError(atPath: path)
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }

                if error != nil {
                    progress.cancel()
                }
                do {
                    try FileManager.default.moveItem(at: tempURL, to: destURL)
                } catch {
                    strongSelf.dispatch_queue.async { [weak self] in
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    try? FileManager.default.removeItem(at: tempURL)
                    return
                }
                
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
            }
        }
        return progress
    }
    
    /**
     특정 Path의 `Data`를 완료 핸들러로 반환하는 기본 메쏘드
     - Parameters:
     - path: `Data`를 가져올 하위 경로.
        - offset: `Data`를 가져올 시작 지점
        - length: 가져올 `Data` 길이
        - completionHandler: 완료 핸들러
        - contents: 가져온 `Data` 반환. 실패시 nil 반환
        - error: 에러 반환
     - Returns: `Progress` 반환. 실패시 nil 반환
     */
    @discardableResult
    open func contents(path: String,
                       offset: Int64,
                       length: Int,
                       completionHandler: @escaping ((_ contents: Data?, _ error: Error?) -> Void)) -> Progress? {
        return self.contents(path: path,
                             offset: offset,
                             length: length,
                             tryCount: nil,
                             completionHandler: completionHandler)
    }
    /**
     특정 Path의 `Data`를 완료 핸들러로 반환하는 확장 메쏘드
     - Parameters:
     - path: `Data`를 가져올 하위 경로.
        - offset: `Data`를 가져올 시작 지점
        - length: 가져올 `Data` 길이
        - tryCount: 재시도 횟수. 기본값은 nil로 최초 실행될 때 0으로 세팅된다. 이후 재귀적으로 실행시 1씩 증가된 값이 지정된다
        - completionHandler: 완료 핸들러
        - contents: 가져온 `Data` 반환. 실패시 nil 반환
        - error: 에러 반환
     - Returns: `Progress` 반환. 실패시 nil 반환
     */
    @discardableResult
    open func contents(path: String,
                       offset: Int64,
                       length: Int,
                       tryCount: Int? = nil,
                       completionHandler: @escaping ((_ contents: Data?, _ error: Error?) -> Void)) -> Progress? {
        let operation = FileOperationType.fetch(path: path)
        if length == 0 || offset < 0 {
            dispatch_queue.async { [weak self] in
                completionHandler(Data(), nil)
                self?.delegateNotify(operation)
            }
            return nil
        }
        
        // 재시도 횟수
        // tryCount가 nil이 아닌 경우, 그 값을 사용. nil인 경우 0으로 지정
        let tryCount = tryCount != nil ? tryCount! : 0

        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        
        //---------------------------------------------------------------------------------------------------//
        /// 에러 발생시 재시도 내부 메쏘드
        /// - Returns: 재시도시 true 반환
        func checkRetry(_ error: Error) -> Bool {
            // URLError인 경우
            if let uerror = error as? URLError {
                print("FTPFileProvider>contents(path:offfset:length:completionHandler:): error code = \(uerror.code)")
                // unsupportedURL / timedOut  에러인 경우
                // rfc3659enabled 를 false 로 해서 재작업 진행
                if uerror.code == .unsupportedURL ||
                    uerror.code == .timedOut {
                    
                    // 에러 재시도 횟수가 5회를 넘었는지 확인
                    guard tryCount < 5 else {
                        // 5회째인 경우, 에러 처리후 종료
                        completionHandler(nil, error)
                        return false
                    }
                    
                    // Progress 전체 개수 증가
                    progress.totalUnitCount += 1

                    // 재시도
                    let appendedProgress = self.contents(path: path,
                                                         offset: offset,
                                                         length: length,
                                                         tryCount: tryCount + 1,
                                                         completionHandler: completionHandler)
                    if appendedProgress != nil {
                        print("FTPFileProvider>contents(path:offfset:length:completionHandler:): 재시도...")
                        progress.addChild(appendedProgress!, withPendingUnitCount: 1)
                        return true
                    }
                }
            }
            // 그 외의 경우
            return false
        }
        //---------------------------------------------------------------------------------------------------//

        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    let error = FileProviderFTPError.unknownError(atPath: path)
                    completionHandler(nil, error)
                }
                return
            }

            if let error = error {
                // 재시도 확인
                if checkRetry(error) == true { return }

                // 재시도 실패시
                strongSelf.dispatch_queue.async {
                    completionHandler(nil, error)
                }
                return
            }
            
            let stream = OutputStream.toMemory()
            strongSelf.ftpDownload(task, filePath: strongSelf.ftpPath(path), from: offset, length: length, to: stream, onTask: { task in
                weak var weakTask = task
                progress.cancellationHandler = {
                    weakTask?.cancel()
                }
                progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
            }, onProgress: { recevied, totalReceived, totalSize in
                // 전송 길이가 지정된 경우
                if length > -1 {
                    progress.totalUnitCount = Int64(length)
                }
                // 지정되지 않은 경우
                else {
                    progress.totalUnitCount = totalSize
                }
                progress.completedUnitCount = totalReceived
#if DEBUG
                print("FTPFileProvider>contents(path:offfset:length:completionHandler:): 진행 = \(progress.fractionCompleted)")
#endif
                strongSelf.delegateNotify(operation, progress: progress.fractionCompleted)
            }) { [weak self] (error) in
                guard let strongSelf = self else {
                    progress.cancel()
                    self?.dispatch_queue.async { [weak self] in
                        let error = FileProviderFTPError.unknownError(atPath: path)
                        completionHandler(nil, error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }

                print("FTPFileProvider>contents(path:offfset:length:completionHandler:): 취소 여부 = \(progress.isCancelled)")
                if let error = error {
                    // 재시도 확인
                    if checkRetry(error) == true { return }

                    // 재시도 실패시
                    
                    progress.cancel()
                    strongSelf.dispatch_queue.async { [weak self] in
                        completionHandler(nil, error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }
                
                if let data = stream.property(forKey: .dataWrittenToMemoryStreamKey) as? Data {
                    strongSelf.dispatch_queue.async { [weak self] in
                        completionHandler(data, nil)
                        self?.delegateNotify(operation)
                    }
                }
            }
        }
        return progress
    }
    
    @discardableResult
    open func writeContents(path: String, contents data: Data?, atomically: Bool, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.modify(path: path)
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        
        let progress = Progress(totalUnitCount: Int64(data?.count ?? -1))
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async { [weak self] in
                    let error = FileProviderFTPError.unknownError(atPath: path)
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }

            if let error = error {
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            
            let storeHandler = {
                let data = data ?? Data()
                let stream = InputStream(data: data)
                strongSelf.ftpStore(task, filePath: strongSelf.ftpPath(path), from: stream, size: Int64(data.count), onTask: { task in
                    weak var weakTask = task
                    progress.cancellationHandler = {
                        weakTask?.cancel()
                    }
                    progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
                }, onProgress: { bytesSent, totalSent, expectedBytes in
                    progress.completedUnitCount = totalSent
                    strongSelf.delegateNotify(operation, progress: progress.fractionCompleted)
                }, completionHandler: { [weak self] (error) in
                    guard let strongSelf = self else {
                        progress.cancel()
                        self?.ftpQuit(task)
                        self?.dispatch_queue.async { [weak self] in
                            let error = FileProviderFTPError.unknownError(atPath: path)
                            completionHandler?(error)
                            self?.delegateNotify(operation, error: error)
                        }
                        return
                    }

                    if error != nil {
                        progress.cancel()
                    }
                    strongSelf.ftpQuit(task)
                    strongSelf.dispatch_queue.async { [weak self] in
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                })
            }
            
            if overwrite {
                storeHandler()
            } else {
                strongSelf.attributesOfItem(path: path, completionHandler: { (file, error) in
                    if file == nil {
                        storeHandler()
                    }
                })
            }
        }
        
        return progress
    }
    
    public func contents(path: String, offset: Int64, length: Int, responseHandler: ((URLResponse) -> Void)?, progressHandler: @escaping (Int64, Data) -> Void, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.fetch(path: path)
        if length == 0 || offset < 0 {
            dispatch_queue.async { [weak self] in
                completionHandler?(nil)
                self?.delegateNotify(operation)
            }
            return nil
        }
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    let error = FileProviderFTPError.unknownError(atPath: path)
                    completionHandler?(error)
                }
                return
            }

            if let error = error {
                strongSelf.dispatch_queue.async {
                    completionHandler?(error)
                }
                return
            }
            
            strongSelf.ftpDownloadData(task, filePath: strongSelf.ftpPath(path), from: offset, length: length, onTask: { task in
                weak var weakTask = task
                progress.cancellationHandler = {
                    weakTask?.cancel()
                }
                progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
            }, onProgress: { [weak self] (data, recevied, totalReceived, totalSize) in
                progressHandler(totalReceived - recevied, data)
                progress.totalUnitCount = totalSize
                progress.completedUnitCount = totalReceived
                self?.delegateNotify(operation, progress: progress.fractionCompleted)
            }) { [weak self] (data, error) in
                guard let strongSelf = self else {
                    progress.cancel()
                    self?.dispatch_queue.async { [weak self] in
                        let error = FileProviderFTPError.unknownError(atPath: path)
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }

                if let error = error {
                    progress.cancel()
                    strongSelf.dispatch_queue.async { [weak self] in
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }
                
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(nil)
                    self?.delegateNotify(operation)
                }
            }
        }
        
        return progress
    }
    
    /**
     Creates a symbolic link at the specified path that points to an item at the given path.
     This method does not traverse symbolic links contained in destination path, making it possible
     to create symbolic links to locations that do not yet exist.
     Also, if the final path component is a symbolic link, that link is not followed.
     
     - Note: Many servers does't support this functionality.
     
     - Parameters:
       - symbolicLink: The file path at which to create the new symbolic link. The last component of the path issued as the name of the link.
       - withDestinationPath: The path that contains the item to be pointed to by the link. In other words, this is the destination of the link.
       - completionHandler: If an error parameter was provided, a presentable `Error` will be returned.
     */
    open func create(symbolicLink path: String, withDestinationPath destPath: String, completionHandler: SimpleCompletionHandler) {
        let operation = FileOperationType.link(link: path, target: destPath)
        _=self.doOperation(operation, completionHandler: completionHandler)
    }
}

extension FTPFileProvider {
    fileprivate func doOperation(_ operation: FileOperationType, completionHandler: SimpleCompletionHandler) -> Progress? {
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        let sourcePath = operation.source
        let destPath = operation.destination
        
        let command: String
        switch operation {
        case .create: command = "MKD \(ftpPath(sourcePath))"
        case .copy: command = "SITE CPFR \(ftpPath(sourcePath))\r\nSITE CPTO \(ftpPath(destPath!))"
        case .move: command = "RNFR \(ftpPath(sourcePath))\r\nRNTO \(ftpPath(destPath!))"
        case .remove: command = "DELE \(ftpPath(sourcePath))"
        case .link: command = "SITE SYMLINK \(ftpPath(sourcePath)) \(ftpPath(destPath!))"
        default: return nil // modify, fetch
        }
        let progress = Progress(totalUnitCount: 1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.fpstreamTask(withHostName: baseURL!.host!, port: baseURL!.port!)
        task.serverTrustPolicy = serverTrustPolicy
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        self.ftpLogin(task) { [weak self] (error) in
            guard let strongSelf = self else {
                let error = FileProviderFTPError.unknownError()
                completionHandler?(error)
                self?.delegateNotify(operation, error: error)
                return
            }

            if let error = error {
                completionHandler?(error)
                strongSelf.delegateNotify(operation, error: error)
                return
            }
            
            strongSelf.execute(command: command, on: task, completionHandler: { [weak self] (response, error) in
                guard let strongSelf = self else {
                    let error = FileProviderFTPError.unknownError()
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                    return
                }

                if let error = error {
                    completionHandler?(error)
                    strongSelf.delegateNotify(operation, error: error)
                    return
                }
                
                guard let response = response else {
                    completionHandler?(error)
                    strongSelf.delegateNotify(operation, error: URLError(.badServerResponse, url: strongSelf.url(of: sourcePath)))
                    return
                }
                
                let codes: [Int] = response.components(separatedBy: .newlines).compactMap({ $0.isEmpty ? nil : $0})
                    .compactMap {
                        let code = $0.components(separatedBy: .whitespaces).compactMap({ $0.isEmpty ? nil : $0}).first
                        return code != nil ? Int(code!) : nil
                }
                
                if codes.filter({ (450..<560).contains($0) }).count > 0 {
                    let errorCode: URLError.Code
                    switch operation {
                    case .create: errorCode = .cannotCreateFile
                    case .modify: errorCode = .cannotWriteToFile
                    case .copy:
                        strongSelf.fallbackCopy(operation, progress: progress, completionHandler: completionHandler)
                        return
                    case .move: errorCode = .cannotMoveFile
                    case .remove:
                        strongSelf.fallbackRemove(operation, progress: progress, on: task, completionHandler: completionHandler)
                        return
                    case .link: errorCode = .cannotWriteToFile
                    default: errorCode = .cannotOpenFile
                    }
                    let error = URLError(errorCode, url: strongSelf.url(of: sourcePath))
                    progress.cancel()
                    completionHandler?(error)
                    strongSelf.delegateNotify(operation, error: error)
                    return
                }
                
                #if os(macOS) || os(iOS) || os(tvOS)
                strongSelf._registerUndo(operation)
                #endif
                progress.completedUnitCount = progress.totalUnitCount
                completionHandler?(nil)
                strongSelf.delegateNotify(operation)
            })
        }
        
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
        return progress
    }
    
    private func fallbackCopy(_ operation: FileOperationType, progress: Progress, completionHandler: SimpleCompletionHandler) {
        let sourcePath = operation.source
        guard let destPath = operation.destination else { return }
        
        let localURL = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString).appendingPathExtension("tmp")
        
        progress.becomeCurrent(withPendingUnitCount: 1)
        _ = self.copyItem(path: sourcePath, toLocalURL: localURL) { [weak self] (error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async { [weak self] in
                    let error = FileProviderFTPError.unknownError()
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }

            if let error = error {
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            
            progress.becomeCurrent(withPendingUnitCount: 1)
            _ = strongSelf.copyItem(localFile: localURL, to: destPath) { [weak self] (error) in
                completionHandler?(nil)
                self?.delegateNotify(operation)
            }
            progress.resignCurrent()
        }
        progress.resignCurrent()
        return
    }
    
    private func fallbackRemove(_ operation: FileOperationType, progress: Progress, on task: FileProviderStreamTask, completionHandler: SimpleCompletionHandler) {
        let sourcePath = operation.source
        
        self.execute(command: "SITE RMDIR \(ftpPath(sourcePath))", on: task) { [weak self] (response, error) in
            guard let strongSelf = self else {
                progress.cancel()
                let error = FileProviderFTPError.unknownError()
                self?.dispatch_queue.async {
                    completionHandler?(error)
                }
                self?.delegateNotify(operation, error: error)
                return
            }

            do {
                if let error = error {
                    throw error
                }
                
                guard let response = response else {
                    throw URLError(.badServerResponse, url: strongSelf.url(of: sourcePath))
                }
                
                if response.hasPrefix("50") {
                    strongSelf.fallbackRecursiveRemove(operation, progress: progress, on: task, completionHandler: completionHandler)
                    return
                }
                
                if !response.hasPrefix("2") {
                    throw URLError(.cannotRemoveFile, url: strongSelf.url(of: sourcePath))
                }
                strongSelf.dispatch_queue.async {
                    completionHandler?(nil)
                }
                strongSelf.delegateNotify(operation)
            } catch {
                progress.cancel()
                strongSelf.dispatch_queue.async {
                    completionHandler?(error)
                }
                strongSelf.delegateNotify(operation, error: error)
            }
        }
    }
    
    private func fallbackRecursiveRemove(_ operation: FileOperationType, progress: Progress, on task: FileProviderStreamTask, completionHandler: SimpleCompletionHandler) {
        let sourcePath = operation.source
        
        _ = self.recursiveList(path: sourcePath, useMLST: true, completionHandler: { [weak self] (contents, error) in
            guard let strongSelf = self else {
                self?.dispatch_queue.async {
                    let error = FileProviderFTPError.unknownError()
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }

            if let error = error {
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                return
            }
            
            progress.becomeCurrent(withPendingUnitCount: 1)
            let recursiveProgress = Progress(totalUnitCount: Int64(contents.count))
            let sortedContents = contents.sorted(by: {
                $0.path.localizedStandardCompare($1.path) == .orderedDescending
            })
            progress.resignCurrent()
            var command = ""
            for file in sortedContents {
                command += (file.isDirectory ? "RMD \(strongSelf.ftpPath(file.path))" : "DELE \(strongSelf.ftpPath(file.path))") + "\r\n"
            }
            command += "RMD \(strongSelf.ftpPath(sourcePath))"
            
            strongSelf.execute(command: command, on: task, completionHandler: { [weak self] (response, error) in
                guard let strongSelf = self else {
                    self?.dispatch_queue.async { [weak self] in
                        let error = FileProviderFTPError.unknownError()
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                    return
                }

                recursiveProgress.completedUnitCount += 1
                strongSelf.dispatch_queue.async { [weak self] in
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
                // TODO: Digest response
            })
        })
    }
}

extension FTPFileProvider: FileProvider { }

#if os(macOS) || os(iOS) || os(tvOS)
extension FTPFileProvider: FileProvideUndoable { }
#endif
