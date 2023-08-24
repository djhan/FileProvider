//
//  HTTPFileProvider.swift
//  FilesProvider
//
//  Created by Amir Abbas Mousavian.
//  Copyright © 2017 Mousavian. Distributed under MIT license.
//

import Foundation
import Cocoa

import CommonLibrary
import ExtendOperation

// MARK: - Enumerations -
/// HTTP Enumerations
enum HTTP {
    /// 에러
    enum Error: LocalizedError {
        /// 연결 실패 에러
        case connection
        /// 인증 실패 에러
        case authentication
        /// 디렉토리를 읽을 수 없음
        case readDirectory
        /// 수신 실패 에러
        case receive
        /// 송신 실패 에러
        case send
        /// 사용자 중지
        case aborted
        /// 알 수 없는 에러
        case unknown
    }
}

/**
 The abstract base class for all REST/Web based providers such as WebDAV, Dropbox, OneDrive, Google Drive, etc. and encapsulates basic
 functionalitis such as downloading/uploading.
 
 No instance of this class should (and can) be created. Use derived classes instead. It leads to a crash with `fatalError()`.
 */
open class HTTPFileProvider: NSObject,
                             FileProviderBasicRemote,
                             FileProviderOperations,
                             FileProviderReadWrite,
                             FileProviderReadWriteProgressive,
                             SerialWorksConvertible {
    
    // MARK: - Properties
    
    /// 작업 큐
    /// - 동시작업 큐 개수는 1개로 제한
    public var workQueue: CountableOperationQueue? = CountableOperationQueue.init(withWorkCount: 1)
    /// 큐 등록 가능 여부
    public var allowRegisterSerialWork = true
    
    open class var type: String { fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.") }
    public let baseURL: URL?
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
    
    fileprivate var _session: URLSession!
    internal fileprivate(set) var sessionDelegate: SessionDelegate?
    public var session: URLSession {
        get {
            if _session == nil {
                self.sessionDelegate = SessionDelegate(fileProvider: self)
                let config = URLSessionConfiguration.default
                config.urlCache = cache
                config.requestCachePolicy = .returnCacheDataElseLoad
                _session = URLSession(configuration: config, delegate: sessionDelegate as URLSessionDelegate?, delegateQueue: self.operation_queue)
                _session.sessionDescription = UUID().uuidString
                initEmptySessionHandler(_session.sessionDescription!)
            }
            return _session
        }
        
        set {
            assert(newValue.delegate is SessionDelegate, "session instances should have a SessionDelegate instance as delegate.")
            _session = newValue
            if _session.sessionDescription?.isEmpty ?? true {
                _session.sessionDescription = UUID().uuidString
            }
            self.sessionDelegate = newValue.delegate as? SessionDelegate
            initEmptySessionHandler(_session.sessionDescription!)
        }
    }
    
    fileprivate var _longpollSession: URLSession?
    /// This session has extended timeout up to 10 minutes, suitable for monitoring.
    internal var longpollSession: URLSession {
        if _longpollSession == nil {
            let config = URLSessionConfiguration.default
            config.timeoutIntervalForRequest = 600
            _longpollSession = URLSession(configuration: config, delegate: nil, delegateQueue: nil)
        }
        return _longpollSession!
    }
    
#if os(macOS) || os(iOS) || os(tvOS)
    open var undoManager: UndoManager? = nil
#endif
    
    /**
     This is parent initializer for subclasses. Using this method on `HTTPFileProvider` will fail as `type` is not implemented.
     
     - Parameters:
     - baseURL: Location of server.
     - credential: An `URLCredential` object with `user` and `password`.
     - cache: A URLCache to cache downloaded files and contents.
     */
    public init(baseURL: URL?, credential: URLCredential?, cache: URLCache?) {
        // Make base url absolute and path as directory
        let urlStr = baseURL?.absoluteString
        self.baseURL = urlStr.flatMap { $0.hasSuffix("/") ? URL(string: $0) : URL(string: $0 + "/") }
        self.useCache = false
        self.validatingCache = true
        self.cache = cache
        self.credential = credential
        
        let queueLabel = "FileProvider.\(Swift.type(of: self).type)"
        dispatch_queue = DispatchQueue(label: queueLabel, attributes: .concurrent)
        operation_queue = OperationQueue()
        operation_queue.name = "\(queueLabel).Operation"
        
        super.init()
    }
    
    public required convenience init?(coder aDecoder: NSCoder) {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
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
        longpollSession.invalidateAndCancel()
    }
    
    public func encode(with aCoder: NSCoder) {
        aCoder.encode(self.baseURL, forKey: "baseURL")
        aCoder.encode(self.credential, forKey: "credential")
        aCoder.encode(self.useCache, forKey: "useCache")
        aCoder.encode(self.validatingCache, forKey: "validatingCache")
    }
    
    public static var supportsSecureCoding: Bool {
        return true
    }
    
    open func copy(with zone: NSZone? = nil) -> Any {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    open func contentsOfDirectory(path: String, completionHandler: @escaping (_ contents: [FileObject], _ error: Error?) -> Void) {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    open func attributesOfItem(path: String, completionHandler: @escaping (_ attributes: FileObject?, _ error: Error?) -> Void) {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    open func storageProperties(completionHandler: @escaping (_ volumeInfo: VolumeObject?) -> Void) {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    @discardableResult
    open func searchFiles(path: String, recursive: Bool, query: NSPredicate, foundItemHandler: ((FileObject) -> Void)?, completionHandler: @escaping (_ files: [FileObject], _ error: Error?) -> Void) -> Progress? {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    open func isReachable(completionHandler: @escaping (_ success: Bool, _ error: Error?) -> Void) {
        self.storageProperties { volume in
            if volume != nil {
                completionHandler(volume != nil, nil)
                return
            } else {
                self.contentsOfDirectory(path: "", completionHandler: { (files, error) in
                    completionHandler(false, error)
                })
            }
        }
    }
    
    // Nothing special for these two funcs, just reimplemented to workaround a bug in swift to allow override in subclasses!
    open func url(of path: String) -> URL {
        var rpath: String = path
        rpath = rpath.addingPercentEncoding(withAllowedCharacters: .filePathAllowed) ?? rpath
        if let baseURL = baseURL {
            if rpath.hasPrefix("/") {
                rpath.remove(at: rpath.startIndex)
            }
            return URL(string: rpath, relativeTo: baseURL) ?? baseURL
        } else {
            return URL(string: rpath) ?? URL(string: "/")!
        }
    }
    
    open func relativePathOf(url: URL) -> String {
        // check if url derieved from current base url
        let relativePath = url.relativePath
        if !relativePath.isEmpty, url.baseURL == self.baseURL {
            return (relativePath.removingPercentEncoding ?? relativePath).replacingOccurrences(of: "/", with: "", options: .anchored)
        }
        
        // resolve url string against baseurl
        guard let baseURL = self.baseURL else { return url.absoluteString }
        let standardRelativePath = url.absoluteString.replacingOccurrences(of: baseURL.absoluteString, with: "/").replacingOccurrences(of: "/", with: "", options: .anchored)
        if URLComponents(string: standardRelativePath)?.host?.isEmpty ?? true {
            return standardRelativePath.removingPercentEncoding ?? standardRelativePath
        } else {
            return relativePath.replacingOccurrences(of: "/", with: "", options: .anchored)
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
        return doOperation(.move(source: path, destination: toPath), overwrite: overwrite, completionHandler: completionHandler)
    }
    
    @discardableResult
    open func copyItem(path: String, to toPath: String, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        return doOperation(.copy(source: path, destination: toPath), overwrite: overwrite, completionHandler: completionHandler)
    }
    
    @discardableResult
    open func removeItem(path: String, completionHandler: SimpleCompletionHandler) -> Progress? {
        return doOperation(.remove(path: path), completionHandler: completionHandler)
    }
    
    @discardableResult
    open func copyItem(localFile: URL, to toPath: String, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
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
        let request = self.request(for: operation, overwrite: overwrite)
        return upload_file(toPath, request: request, localFile: localFile, operation: operation, completionHandler: completionHandler)
    }
    
    @discardableResult
    open func copyItem(path: String, toLocalURL destURL: URL, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.copy(source: path, destination: destURL.absoluteString)
        let request = self.request(for: operation)
        let cantLoadError = URLError(.cannotLoadFromNetwork, url: self.url(of: path))
        return self.download_file(path: path, request: request, operation: operation, completionHandler: { [weak self] (tempURL, error) in
            do {
                if let error = error {
                    throw error
                }
                
                guard let tempURL = tempURL else {
                    throw cantLoadError
                }
                
#if os(macOS) || os(iOS) || os(tvOS)
                var coordError: NSError?
                NSFileCoordinator().coordinate(writingItemAt: tempURL, options: .forMoving, writingItemAt: destURL, options: .forReplacing, error: &coordError, byAccessor: { (tempURL, destURL) in
                    do {
                        try FileManager.default.moveItem(at: tempURL, to: destURL)
                        completionHandler?(nil)
                        self?.delegateNotify(operation)
                    } catch {
                        completionHandler?(error)
                        self?.delegateNotify(operation, error: error)
                    }
                })
                
                if let error = coordError {
                    throw error
                }
#else
                do {
                    try FileManager.default.moveItem(at: tempURL, to: destURL)
                    completionHandler?(nil)
                    self?.delegateNotify(operation)
                } catch {
                    completionHandler?(error)
                    self?.delegateNotify(operation, error: error)
                }
#endif
                
            } catch {
                completionHandler?(error)
                self?.delegateNotify(operation, error: error)
            }
        })
    }
    
    /**
     Progressively fetch data of file and returns fetched data in `progressHandler`.
     If path specifies a directory, or if some other error occurs, data will be nil.
     
     - Parameters:
     - path: Path of file.
     - progressHandler: a closure called every time a new `Data` is available.
     - position: start position of data fetched.
     - data: a portion of contents of file in a `Data` object.
     - completionHandler: a closure with result of file contents or error.
     - error: `Error` returned by system if occured.
     - Returns: An `Progress` to get progress or cancel progress.
     */
    @discardableResult
    open func contents(path: String, offset: Int64 = 0, length: Int = -1, responseHandler: ((_ response: URLResponse) -> Void)? = nil, progressHandler: @escaping (_ position: Int64, _ data: Data) -> Void, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.fetch(path: path)
        var request = self.request(for: operation)
        request.setValue(rangeWithOffset: offset, length: length)
        var position: Int64 = offset
        return download_progressive(path: path, request: request, operation: operation, offset: offset, responseHandler: responseHandler, progressHandler: { data in
            progressHandler(position, data)
            position += Int64(data.count)
        }, completionHandler: (completionHandler ?? { _ in return }))
    }
    
    @discardableResult
    open func contents(path: String, offset: Int64, length: Int, completionHandler: @escaping ((_ contents: Data?, _ error: Error?) -> Void)) -> Progress? {
        if length == 0 || offset < 0 {
            dispatch_queue.async {
                completionHandler(Data(), nil)
            }
            return nil
        }
        
        let operation = FileOperationType.fetch(path: path)
        var request = self.request(for: operation)
        let cantLoadError = URLError(.cannotLoadFromNetwork, url: self.url(of: path))
        request.setValue(rangeWithOffset: offset, length: length)
        
        let stream = OutputStream.toMemory()
        return self.download(path: path, request: request, operation: operation, offset: offset, stream: stream) { (error) in
            do {
                if let error = error {
                    throw error
                }
                
                guard let data = stream.property(forKey: .dataWrittenToMemoryStreamKey) as? Data else {
                    throw cantLoadError
                }
                completionHandler(data, nil)
            } catch {
                completionHandler(nil, error)
            }
        }
    }
    
    @discardableResult
    open func writeContents(path: String, contents data: Data?, atomically: Bool, overwrite: Bool, completionHandler: SimpleCompletionHandler) -> Progress? {
        let operation = FileOperationType.modify(path: path)
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        let data = data ?? Data()
        let request = self.request(for: operation, overwrite: overwrite, attributes: [.contentModificationDateKey: Date()])
        let stream = InputStream(data: data)
        return upload(path, request: request, stream: stream, size: Int64(data.count), operation: operation, completionHandler: completionHandler)
    }
    
    internal func request(for operation: FileOperationType, overwrite: Bool = false, attributes: [URLResourceKey: Any] = [:]) -> URLRequest {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    internal func serverError(with code: FileProviderHTTPErrorCode, path: String?, data: Data?) -> FileProviderHTTPError {
        fatalError("HTTPFileProvider is an abstract class. Please implement \(#function) in subclass.")
    }
    
    internal func multiStatusError(operation: FileOperationType, data: Data) -> FileProviderHTTPError? {
        // WebDAV will override this function
        return nil
    }
    
    /**
     This is the main function to init urlsession task for specified file operation.
     
     You won't need to override this function unless another network request must be done before intended operation,
     such as retrieving file id from file path. Then you must call `super.doOperation()`
     
     In case you have to call super method asyncronously, create a `Progress` object and pass ot to `progress` parameter.
     */
    @discardableResult
    internal func doOperation(_ operation: FileOperationType, overwrite: Bool = false, progress: Progress? = nil,
                              completionHandler: SimpleCompletionHandler) -> Progress? {
        guard fileOperationDelegate?.fileProvider(self, shouldDoOperation: operation) ?? true == true else {
            return nil
        }
        
        let progress = progress ?? Progress(totalUnitCount: 1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let request = self.request(for: operation, overwrite: overwrite)
        
        let task = session.dataTask(with: request, completionHandler: { (data, response, error) in
            do {
                if let error = error {
                    throw error
                }
                
                if let response = response as? HTTPURLResponse {
                    if response.statusCode >= 300, let code = FileProviderHTTPErrorCode(rawValue: response.statusCode) {
                        throw self.serverError(with: code, path: operation.source, data: data)
                    }
                    
                    if FileProviderHTTPErrorCode(rawValue: response.statusCode) == .multiStatus, let data = data,
                       let ms_error = self.multiStatusError(operation: operation, data: data) {
                        throw ms_error
                    }
                }
                
#if os(macOS) || os(iOS) || os(tvOS)
                self._registerUndo(operation)
#endif
                progress.completedUnitCount = 1
                completionHandler?(nil)
                self.delegateNotify(operation)
            } catch {
                completionHandler?(error)
                self.delegateNotify(operation, error: error)
            }
        })
        task.taskDescription = operation.json
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
        task.resume()
        return progress
    }
    
    // codebeat:disable[ARITY]
    
    /**
     This method should be used in subclasses to fetch directory content from servers which support paginated results.
     Almost all HTTP based provider, except WebDAV, supports this method.
     
     - Important: Please use `[weak self]` when implementing handlers to prevent retain cycles. In these cases,
     return `nil` as the result of handler as the operation will be aborted.
     
     - Parameters:
     - path: path of directory which enqueued for listing, for informational use like errpr reporting.
     - requestHandler: Get token of next page and returns appropriate `URLRequest` to be sent to server.
     handler can return `nil` to cancel entire operation.
     - token: Token of the page which `URLRequest` is needed, token will be `nil` for initial page.
     - pageHandler: Handler which is called after fetching results of a page to parse data. will return parse result as
     array of `FileObject` or error if data is nil or parsing is failed. Method will not continue to next page if
     `error` is returned, otherwise `nextToken` will be used for next page. `nil` value for `newToken` will indicate
     last page of directory contents.
     - data: Raw data returned from server. Handler should parse them and return files.
     - progress: `Progress` object that `completedUnits` will be increased when a new `FileObject` is parsed in method.
     - completionHandler: All file objects returned by `pageHandler` will be passed to this handler, or error if occured.
     This handler will be called when `pageHandler` returns `nil for `newToken`.
     - contents: all files parsed via `pageHandler` will be return aggregated.
     - error: `Error` returned by server. `nil` means success. If exists, it means `contents` are incomplete.
     */
    internal func paginated(_ path: String, requestHandler: @escaping (_ token: String?) -> URLRequest?,
                            pageHandler: @escaping (_ data: Data?, _ progress: Progress) -> (files: [FileObject], error: Error?, newToken: String?),
                            completionHandler: @escaping (_ contents: [FileObject], _ error: Error?) -> Void) -> Progress {
        let progress = Progress(totalUnitCount: -1)
        self.paginated(path, startToken: nil, currentProgress: progress, previousResult: [], requestHandler: requestHandler, pageHandler: pageHandler, completionHandler: completionHandler)
        return progress
    }
    
    private func paginated(_ path: String, startToken: String?, currentProgress progress: Progress,
                           previousResult: [FileObject], requestHandler: @escaping (_ token: String?) -> URLRequest?,
                           pageHandler: @escaping (_ data: Data?, _ progress: Progress) -> (files: [FileObject], error: Error?, newToken: String?),
                           completionHandler: @escaping (_ contents: [FileObject], _ error: Error?) -> Void) {
        guard !progress.isCancelled, let request = requestHandler(startToken) else {
            return
        }
        
        let task = session.dataTask(with: request, completionHandler: { (data, response, error) in
            do {
                if let error = error {
                    throw error
                }
                if let code = (response as? HTTPURLResponse)?.statusCode , code >= 300, let rCode = FileProviderHTTPErrorCode(rawValue: code) {
                    throw self.serverError(with: rCode, path: path, data: data)
                }
                
                let (newFiles, err, newToken) = pageHandler(data, progress)
                if let error = err {
                    throw error
                }
                
                let files = previousResult + newFiles
                if let newToken = newToken, !progress.isCancelled {
                    self.paginated(path, startToken: newToken, currentProgress: progress, previousResult: files, requestHandler: requestHandler, pageHandler: pageHandler, completionHandler: completionHandler)
                } else {
                    completionHandler(files, nil)
                }
            } catch {
                completionHandler(previousResult, error)
            }
        })
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
        task.resume()
    }
    // codebeat:enable[ARITY]
    
    internal var maxUploadSimpleSupported: Int64 { return Int64.max }
    
    func upload_task(_ targetPath: String, progress: Progress, task: URLSessionTask, operation: FileOperationType,
                     completionHandler: SimpleCompletionHandler) -> Void {
        
        var allData = Data()
        dataCompletionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { data in
            allData.append(data)
        }
        
        completionHandlersForTasks[self.session.sessionDescription!]?[task.taskIdentifier] = { [weak self] error in
            var responseError: FileProviderHTTPError?
            if let code = (task.response as? HTTPURLResponse)?.statusCode , code >= 300, let rCode = FileProviderHTTPErrorCode(rawValue: code) {
                responseError = self?.serverError(with: rCode, path: targetPath, data: allData)
            }
            if !(responseError == nil && error == nil) {
                progress.cancel()
            }
            completionHandler?(responseError ?? error)
            self?.delegateNotify(operation, error: responseError ?? error)
        }
        task.taskDescription = operation.json
        sessionDelegate?.observerProgress(of: task, using: progress, kind: .upload)
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
        task.resume()
    }
    
    func upload(_ targetPath: String, request: URLRequest, stream: InputStream, size: Int64, operation: FileOperationType,
                completionHandler: SimpleCompletionHandler) -> Progress? {
        if size > maxUploadSimpleSupported {
            let error = self.serverError(with: .payloadTooLarge, path: targetPath, data: nil)
            completionHandler?(error)
            self.delegateNotify(operation, error: error)
            return nil
        }
        
        let progress = Progress(totalUnitCount: size)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        var request = request
        request.httpBodyStream = stream
        let task = session.uploadTask(withStreamedRequest: request)
        self.upload_task(targetPath, progress: progress, task: task, operation: operation, completionHandler: completionHandler)
        
        return progress
    }
    
    func upload_file(_ targetPath: String, request: URLRequest, localFile: URL, operation: FileOperationType,
                     completionHandler: SimpleCompletionHandler) -> Progress? {
        let fSize = (try? localFile.resourceValues(forKeys: [.fileSizeKey]))?.allValues[.fileSizeKey] as? Int64
        let size = Int64(fSize ?? -1)
        if size > maxUploadSimpleSupported {
            let error = self.serverError(with: .payloadTooLarge, path: targetPath, data: nil)
            completionHandler?(error)
            self.delegateNotify(operation, error: error)
            return nil
        }
        
        let progress = Progress(totalUnitCount: size)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
#if os(macOS) || os(iOS) || os(tvOS)
        var error: NSError?
        NSFileCoordinator().coordinate(readingItemAt: localFile, options: .forUploading, error: &error, byAccessor: { (url) in
            let task = self.session.uploadTask(with: request, fromFile: localFile)
            self.upload_task(targetPath, progress: progress, task: task, operation: operation, completionHandler: completionHandler)
        })
        if let error = error {
            completionHandler?(error)
        }
#else
        self.upload_task(targetPath, progress: progress, task: task, operation: operation, completionHandler: completionHandler)
#endif
        
        return progress
    }
    
    internal func download(path: String, request: URLRequest, operation: FileOperationType,
                           offset: Int64 = 0,
                           responseHandler: ((_ response: URLResponse) -> Void)? = nil,
                           stream: OutputStream,
                           completionHandler: @escaping (_ error: Error?) -> Void) -> Progress? {
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        /**
         # 주의사항:
         - 충돌 방지를 위해 작업을 직렬화, 한 번에 하나씩만 받도록 한다
         - 단, 미리 task를 생성해서 등록하면 RemoteSessiono 에서 잘못된 task.identifier 를 수령하게 된다
         - 따라서 Operation에서 다운로드 실행시 task를 생성해서 실행해야 한다
         */
        guard let operation = HTTPDownloadOperation.init(at: self,
                                                         path: path,
                                                         request: request,
                                                         operation: operation,
                                                         offset: offset,
                                                         progress: progress,
                                                         responseHandler: responseHandler,
                                                         stream: stream,
                                                         completionHandler: completionHandler),
              self.addSerialWork(operation) == true else {
            // 알 수 없는 에러
            completionHandler(HTTP.Error.unknown)
            return nil
        }
        
        /*
         let task = session.dataTask(with: request)
         
         /// # 주의사항
         /// - EXC_BAD_ACCESS 발생. 동일한 다운로드 작업을 동시에 처리할 때 문제가 있는 것으로 판단됨
         /// - 동기화 처리. 단, 문제가 생기지 않는지 확인 필요
         self.syncQueue.async(flags: .barrier) { [weak self] in
         guard let strongSelf = self else {
         return completionHandler(nil)
         }
         
         if let responseHandler = responseHandler {
         responseCompletionHandlersForTasks[strongSelf.session.sessionDescription!]?[task.taskIdentifier] = { response in
         responseHandler(response)
         }
         }
         
         stream.open()
         dataCompletionHandlersForTasks[strongSelf.session.sessionDescription!]?[task.taskIdentifier] = { [weak task, weak self] data in
         guard !data.isEmpty else { return }
         task.flatMap { self?.delegateNotify(operation, progress: Double($0.countOfBytesReceived) / Double($0.countOfBytesExpectedToReceive)) }
         
         let result = (try? stream.write(data: data)) ?? -1
         if result < 0 {
         completionHandler(stream.streamError!)
         self?.delegateNotify(operation, error: stream.streamError!)
         task?.cancel()
         }
         }
         
         completionHandlersForTasks[strongSelf.session.sessionDescription!]?[task.taskIdentifier] = { error in
         if error != nil {
         progress.cancel()
         }
         stream.close()
         completionHandler(error)
         strongSelf.delegateNotify(operation, error: error)
         }
         
         task.taskDescription = operation.json
         strongSelf.sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
         }
         
         progress.cancellationHandler = { [weak task] in
         task?.cancel()
         }
         progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
         task.resume()
         */
        return progress
    }
    
    internal func download_progressive(path: String, request: URLRequest, operation: FileOperationType,
                                       offset: Int64 = 0,
                                       responseHandler: ((_ response: URLResponse) -> Void)? = nil,
                                       progressHandler: @escaping (_ data: Data) -> Void,
                                       completionHandler: @escaping (_ error: Error?) -> Void) -> Progress? {
        /*
         guard let sessionDescription = session.sessionDescription else {
         completionHandler(HTTP.Error.unknown)
         return nil
         }*/
        
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        /**
         # 주의사항:
         - 충돌 방지를 위해 작업을 직렬화, 한 번에 하나씩만 받도록 한다
         - 단, 미리 task를 생성해서 등록하면 RemoteSessiono 에서 잘못된 task.identifier 를 수령하게 된다
         - 따라서 Operation에서 다운로드 실행시 task를 생성해서 실행해야 한다
         */
        guard let operation = HTTPDownloadOperation.init(at: self,
                                                         path: path,
                                                         request: request,
                                                         operation: operation,
                                                         offset: offset,
                                                         progress: progress,
                                                         responseHandler: responseHandler,
                                                         progressHandler: progressHandler,
                                                         completionHandler: completionHandler),
              self.addSerialWork(operation) == true else {
            // 알 수 없는 에러
            completionHandler(HTTP.Error.unknown)
            return nil
        }
        
        /*
         let task = session.dataTask(with: request)
         if let responseHandler = responseHandler {
         registerResponseCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { response in
         responseHandler(response)
         } completion: {
         // 등록 완료
         }
         
         /*
          responseCompletionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { response in
          responseHandler(response)
          }
          */
         }
         
         registerDataCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak task, weak self] data in
         task.flatMap { self?.delegateNotify(operation, progress: Double($0.countOfBytesReceived) / Double($0.countOfBytesExpectedToReceive)) }
         progressHandler(data)
         // 등록 완료
         } completion: {
         registerCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak self] error in
         if error != nil {
         progress.cancel()
         }
         completionHandler(error)
         self?.delegateNotify(operation, error: error)
         
         // 등록 완료
         } completion: { [weak self] in
         task.taskDescription = operation.json
         self?.sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
         progress.cancellationHandler = { [weak task] in
         task?.cancel()
         }
         progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
         task.resume()
         }
         }*/
        
        /*
         dataCompletionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { [weak task, weak self] data in
         task.flatMap { self?.delegateNotify(operation, progress: Double($0.countOfBytesReceived) / Double($0.countOfBytesExpectedToReceive)) }
         progressHandler(data)
         }
         
         completionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { error in
         if error != nil {
         progress.cancel()
         }
         completionHandler(error)
         self.delegateNotify(operation, error: error)
         }
         
         task.taskDescription = operation.json
         sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
         progress.cancellationHandler = { [weak task] in
         task?.cancel()
         }
         progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
         task.resume()
         */
        return progress
    }
    
    internal func download_file(path: String, request: URLRequest, operation: FileOperationType,
                                completionHandler: @escaping ((_ tempURL: URL?, _ error: Error?) -> Void)) -> Progress? {
        let progress = Progress(totalUnitCount: -1)
        progress.setUserInfoObject(operation, forKey: .fileProvderOperationTypeKey)
        progress.kind = .file
        progress.setUserInfoObject(Progress.FileOperationKind.downloading, forKey: .fileOperationKindKey)
        
        let task = session.downloadTask(with: request)
        completionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { error in
            if let error = error {
                progress.cancel()
                completionHandler(nil, error)
                self.delegateNotify(operation, error: error)
            }
        }
        downloadCompletionHandlersForTasks[session.sessionDescription!]?[task.taskIdentifier] = { tempURL in
            guard let httpResponse = task.response as? HTTPURLResponse , httpResponse.statusCode < 300 else {
                let code = FileProviderHTTPErrorCode(rawValue: (task.response as? HTTPURLResponse)?.statusCode ?? -1)
                let errorData : Data? = try? Data(contentsOf: tempURL)
                let serverError = code.flatMap { self.serverError(with: $0, path: path, data: errorData) }
                if serverError != nil {
                    progress.cancel()
                }
                completionHandler(nil, serverError)
                self.delegateNotify(operation)
                return
            }
            
            completionHandler(tempURL, nil)
        }
        task.taskDescription = operation.json
        sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
        progress.cancellationHandler = { [weak task] in
            task?.cancel()
        }
        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
        task.resume()
        return progress
    }
}

extension HTTPFileProvider: FileProvider { }

#if os(macOS) || os(iOS) || os(tvOS)
extension HTTPFileProvider: FileProvideUndoable { }
#endif


// MARK: - HTTP Asynchronous Operation -
class HTTPDownloadOperation: DefaultAsynchronousOperation {
    
    /// provider
    weak private var provider: HTTPFileProvider?
    /// 세션
    weak private var session: URLSession?
    /// Stream
    weak private var stream: OutputStream?
    /// progress
    weak var progress: Progress?
    
    /// 요청
    private var request: URLRequest
    /// 다운로드받을 파일의 서버측 하위 경로
    private var path: String
    /// 작업 종류
    private var operation: FileOperationType
    /// offset
    private var offset: Int64 = 0
    
    /// 반응 핸들러
    private var responseHandler: ((_ response: URLResponse) -> Void)?
    /// 진행 핸들러
    private var progressHandler: ((_ data: Data) -> Void)?
    /// 완료 핸들러
    private var completionHandler: (_ error: Error?) -> Void
    
    /// Data Task
    /// - Note: ventura에서 지역변수로 지정한 task가 nil로 떨어지는 일이 발생. 클래스 변수로 지정해서 저장하도록 한다
    private var task: URLSessionDataTask?
    
    /**
     다운로드 등록
     - Parameters:
     - provider: `HTTPFileProvider`
     - path: `String`
     - request: `URLRequest`
     - operation: `FileOperationType` 을 지정
     - offset: offset 값이 있는 경우 지정, 기본값은 0
     - progress: `Progress` 지정
     - responseHandler: `URLResponse` 반환 핸들러 지정. 옵셔널
     - progressHandler: `Data`를 점진적으로 반환하는 핸들러. 점진적 다운로드시 지정. 옵셔널
     - stream: 다운로드받은 데이터를 저장할 `OutputStream`. 점진적 다운로드시 미지정
     - completionHandler: 완료 핸들러 지정
     */
    init?(at provider: HTTPFileProvider,
          path: String,
          request: URLRequest,
          operation: FileOperationType,
          offset: Int64 = 0,
          progress: Progress,
          responseHandler: ((_ response: URLResponse) -> Void)? = nil,
          progressHandler: ((_ data: Data) -> Void)? = nil,
          stream: OutputStream? = nil,
          completionHandler: @escaping (_ error: Error?) -> Void) {
        
        self.provider = provider
        guard let session = self.provider?.session else {
            return nil
        }
        self.session = session
        self.stream = stream
        self.path = path
        self.request = request
        self.operation = operation
        self.offset = offset
        self.progress = progress
        self.responseHandler = responseHandler
        self.progressHandler = progressHandler
        self.completionHandler = completionHandler
        
        super.init()
        
        if self.progressHandler != nil {
            // progressHandler 지정시
            self.executeHandler = progressDownload
        }
        else {
            // progressHandler 미지정시
            self.executeHandler = download
        }
    }
    
    /// 작업 종료
    private func finishWork(_ error: Error?) {
        if let error = error {
            if #available(macOS 11.0, *) {
                EdgeLogger.shared.networkLogger.log(level: .error, "에러 발생 = \(error.localizedDescription)")
            } else {
                // Fallback on earlier versions
            }
        }
        self.completionHandler(error)
        self.finish()
    }
    
    /// 다운로드 작업 실행
    private func download() {
        guard let provider = self.provider,
              let session = self.session,
              let sessionDescription = session.sessionDescription,
              let stream = self.stream else { //,
            /// # 변경사항
            /// - MakeCacheImage에서 progress 값을 가지고 있지 않기 때문에, progress는 확인하지 않도록 한다
            //let progress = self.progress else {
            self.finishWork(HTTP.Error.unknown)
            return
        }
        
        provider.attributesOfItem(path: path) { [weak self] attributes, error in
            
            if let error = error {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .error, "에러 발생 = \(error.localizedDescription)")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(error)
                return
            }
            guard let strongSelf = self,
                  let size = attributes?.size else {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .debug, "파일 크기를 구할 수 없음, 중지.")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(HTTP.Error.receive)
                return
            }
            
            let offset = strongSelf.offset
            
            strongSelf.progress?.totalUnitCount = size
            // offset 만큼 완료 처리
            strongSelf.progress?.completedUnitCount += offset
            
            if #available(macOS 11.0, *) {
                EdgeLogger.shared.networkLogger.log(level: .debug, "다운로드 개시.")
            } else {
                // Fallback on earlier versions
            }
            
            strongSelf.task = session.dataTask(with: strongSelf.request)
            guard let task = strongSelf.task else {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .debug, "Task 초기화 실패. 중지.")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(HTTP.Error.unknown)
                return
            }
            
            if let responseHandler = strongSelf.responseHandler {
                responseCompletionHandlersForTasks[sessionDescription]?[task.taskIdentifier] = { response in
                    responseHandler(response)
                }
            }
            
            stream.open()
            
            registerDataCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak self, weak task, weak stream, weak provider] data in
                guard !data.isEmpty else { return }
                guard let strongSelf = self,
                      let task = task,
                      let provider = provider,
                      let stream = stream else {
                    self?.finishWork(HTTP.Error.unknown)
                    return
                }
                
                //                task.flatMap {
                //                    provider.delegateNotify(strongSelf.operation, progress: Double($0.countOfBytesReceived) / Double($0.countOfBytesExpectedToReceive))
                //                    strongSelf.progress?.completedUnitCount = $0.countOfBytesReceived + offset
                //                }
                provider.delegateNotify(strongSelf.operation, progress: Double(task.countOfBytesReceived) / Double(task.countOfBytesExpectedToReceive))
                strongSelf.progress?.completedUnitCount = task.countOfBytesReceived + offset
                
                let result = (try? stream.write(data: data)) ?? -1
                if result < 0 {
                    if #available(macOS 11.0, *) {
                        EdgeLogger.shared.networkLogger.log(level: .debug, "작업 취소 처리.")
                    } else {
                        // Fallback on earlier versions
                    }
                    strongSelf.completionHandler(stream.streamError)
                    provider.delegateNotify(strongSelf.operation, error: stream.streamError)
                    task.cancel()
                    strongSelf.finish()
                }
                
                // 등록 완료
            } completion: {
                registerCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak self] error in
                    guard let strongSelf = self else {
                        self?.finishWork(HTTP.Error.unknown)
                        return
                    }
                    
                    if let error = error {
                        strongSelf.progress?.cancel()
                        if #available(macOS 11.0, *) {
                            EdgeLogger.shared.networkLogger.log(level: .error, "에러 발생 = \(error.localizedDescription).")
                        } else {
                            // Fallback on earlier versions
                        }
                    }
                    strongSelf.stream?.close()
                    
                    if #available(macOS 11.0, *) {
                        EdgeLogger.shared.networkLogger.log(level: .debug, "다운로드 종료 처리.")
                    } else {
                        // Fallback on earlier versions
                    }
                    strongSelf.completionHandler(error)
                    strongSelf.provider?.delegateNotify(strongSelf.operation, error: error)
                    // 작업 종료
                    strongSelf.finish()
                    
                    // 등록 완료
                } completion: { [weak self, weak task] in
                    guard let strongSelf = self,
                          let task = task else {
                        self?.finishWork(HTTP.Error.unknown)
                        return
                    }
                    
                    task.taskDescription = strongSelf.operation.json
                    if let progress = strongSelf.progress {
                        strongSelf.provider?.sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
                        progress.cancellationHandler = { [weak task] in
                            task?.cancel()
                        }
                        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
                    }
                    
                    task.resume()
                    if #available(macOS 11.0, *) {
                        EdgeLogger.shared.networkLogger.log(level: .debug, "\(task.taskIdentifier) 번째 Task 실행.")
                    } else {
                        // Fallback on earlier versions
                    }
                }
            }
        }
    }
    
    /// 점진적 다운로드 작업 실행
    private func progressDownload() {
        
        guard let provider = self.provider,
              let session = self.session,
              let sessionDescription = session.sessionDescription else { //,
            /// # 변경사항
            /// - MakeCacheImage에서 progress 값을 가지고 있지 않기 때문에, progress는 확인하지 않도록 한다
            //let progress = self.progress else {
            self.finishWork(HTTP.Error.unknown)
            return
        }
        
        provider.attributesOfItem(path: path) { [weak self] attributes, error in
            
            if let error = error {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .error, "에러 발생 = \(error.localizedDescription).")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(error)
                return
            }
            guard let strongSelf = self,
                  let size = attributes?.size else {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .debug, "파일 크기를 구할 수 없음. 중지.")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(HTTP.Error.receive)
                return
            }
            
            let offset = strongSelf.offset
            
            strongSelf.progress?.totalUnitCount = size
            // offset 만큼 완료 처리
            strongSelf.progress?.completedUnitCount += offset
            
            if #available(macOS 11.0, *) {
                EdgeLogger.shared.networkLogger.log(level: .debug, " 점진적 다운로드 개시.")
            } else {
                // Fallback on earlier versions
            }
            
            strongSelf.task = session.dataTask(with: strongSelf.request)
            guard let task = strongSelf.task else {
                if #available(macOS 11.0, *) {
                    EdgeLogger.shared.networkLogger.log(level: .debug, " Task 초기화 실패. 중지.")
                } else {
                    // Fallback on earlier versions
                }
                self?.finishWork(HTTP.Error.unknown)
                return
            }
            
            if let responseHandler = strongSelf.responseHandler {
                registerResponseCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { response in
                    responseHandler(response)
                } completion: {
                    // 등록 완료
                }
            }
            
            registerDataCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak task, weak self, weak provider] data in
                guard let strongSelf = self,
                      let task = task,
                      let provider = provider else {
                    self?.finishWork(HTTP.Error.unknown)
                    return
                }
                
                //                task.flatMap {
                //                    provider.delegateNotify(strongSelf.operation, progress: Double($0.countOfBytesReceived) / Double($0.countOfBytesExpectedToReceive))
                //                    progress.completedUnitCount = $0.countOfBytesReceived + offset
                //                }
                provider.delegateNotify(strongSelf.operation, progress: Double(task.countOfBytesReceived) / Double(task.countOfBytesExpectedToReceive))
                strongSelf.progress?.completedUnitCount = task.countOfBytesReceived + offset
                strongSelf.progressHandler?(data)
                
                // 등록 완료
            } completion: {
                registerCompletionHandlersForTasks(session: sessionDescription, task: task.taskIdentifier) { [weak self] error in
                    guard let strongSelf = self else {
                        self?.finishWork(HTTP.Error.unknown)
                        return
                    }
                    
                    if let error = error {
                        self?.progress?.cancel()
                        if #available(macOS 11.0, *) {
                            EdgeLogger.shared.networkLogger.log(level: .error, "에러 발생 = \(error.localizedDescription).")
                        } else {
                            // Fallback on earlier versions
                        }
                    }
                    strongSelf.completionHandler(error)
                    strongSelf.provider?.delegateNotify(strongSelf.operation, error: error)
                    strongSelf.finishWork(error)
                    
                    // 등록 완료
                } completion: { [weak self, weak task] in
                    guard let strongSelf = self,
                          let task = task else {
                        self?.finishWork(HTTP.Error.unknown)
                        return
                    }
                    
                    task.taskDescription = strongSelf.operation.json
                    if let progress = strongSelf.progress {
                        strongSelf.provider?.sessionDelegate?.observerProgress(of: task, using: progress, kind: .download)
                        progress.cancellationHandler = { [weak task] in
                            task?.cancel()
                        }
                        progress.setUserInfoObject(Date(), forKey: .startingTimeKey)
                    }
                    task.resume()
                    if #available(macOS 11.0, *) {
                        EdgeLogger.shared.networkLogger.log(level: .debug, "\(task.taskIdentifier) 번째 Task 실행.")
                    } else {
                        // Fallback on earlier versions
                    }
                }
            }
        }
    }
}
