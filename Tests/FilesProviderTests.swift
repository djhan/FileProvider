//
//  FilesProviderTests.swift
//  FilesProviderTests
//
//  Created by Amir Abbas on 8/11/1396 AP.
//
/*
import XCTest
import FilesProvider

class FilesProviderTests: XCTestCase, FileProviderDelegate {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
        try? FileManager.default.removeItem(at: dummyFile())
    }
    
    func testLocal() {
        let provider = LocalFileProvider()
        addTeardownBlock {
            self.testRemoveFile(provider, filePath: self.testFolderName)
        }
        testBasic(provider)
        testArchiving(provider)
        testOperations(provider)
    }
    
    func testWebDav() {
        guard let urlStr = ProcessInfo.processInfo.environment["webdav_url"] else { return }
        let url = URL(string: urlStr)!
        let cred: URLCredential?
        if let user = ProcessInfo.processInfo.environment["webdav_user"], let pass = ProcessInfo.processInfo.environment["webdav_password"] {
            cred = URLCredential(user: user, password: pass, persistence: .forSession)
        } else {
            cred = nil
        }
        let provider = WebDAVFileProvider(baseURL: url, credential: cred, credentialType: .basic)!
        provider.delegate = self
        testArchiving(provider)
        addTeardownBlock {
            self.testRemoveFile(provider, filePath: self.testFolderName)
        }
        testOperations(provider)
    }
    
    func testDropbox() {
        guard let pass = ProcessInfo.processInfo.environment["dropbox_token"] else {
            return
        }
        let cred = URLCredential(user: "testuser", password: pass, persistence: .forSession)
        let provider = DropboxFileProvider(credential: cred)
        provider.delegate = self
        testArchiving(provider)
        addTeardownBlock {
            self.testRemoveFile(provider, filePath: self.testFolderName)
        }
        testBasic(provider)
        testOperations(provider)
    }
    
    func testFTPPassive() {
        /*
        guard let urlStr = ProcessInfo.processInfo.environment["ftp_url"] else { return }
        let url = URL(string: urlStr)!
        let cred: URLCredential?
        if let user = ProcessInfo.processInfo.environment["ftp_user"], let pass = ProcessInfo.processInfo.environment["ftp_password"] {
            cred = URLCredential(user: user, password: pass, persistence: .forSession)
        } else {
            cred = nil
        }
        */
        let url = URL(string: "ftp://ftp.edmapplication.com")!
        let cred = URLCredential(user: "abbas@edmapplication.com", password: "baTsivWZ4", persistence: .forSession)
        //let url = URL(string: "ftpes://ftp.adidas-group.com:21")!
        //let cred = URLCredential(user: "ecomwe-reversals-full", password: "rNeUj726Xqk2k", persistence: .forSession)
        let provider = FTPFileProvider(baseURL: url, mode: .extendedPassive, encoding: .utf8, credential: cred)!
        provider.delegate = self
        testArchiving(provider)
        addTeardownBlock {
            self.testRemoveFile(provider, filePath: self.testFolderName)
        }
        testOperations(provider)
    }
    
    func testOneDrive() {
        guard let pass = ProcessInfo.processInfo.environment["onedrive_token"] else {
            return
        }
        let cred = URLCredential(user: "testuser", password: pass, persistence: .forSession)
        let provider = OneDriveFileProvider(credential: cred)
        provider.delegate = self
        testBasic(provider)
        testArchiving(provider)
        addTeardownBlock {
            self.testRemoveFile(provider, filePath: self.testFolderName)
        }
        testOperations(provider)
    }
    
    /*
    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }
    */
    
    let timeout: Double = 60.0
    let testFolderName = "Test"
    let textFilePath = "/Test/file.txt"
    let renamedFilePath = "/Test/renamed.txt"
    let uploadFilePath = "/Test/uploaded.dat"
    let sampleText = "Hello world!"
    
    fileprivate func testCreateFolder(_ provider: FileProvider, folderName: String) {
        let desc = "Creating folder at root in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.create(folder: folderName, at: "/") { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testContentsOfDirectory(_ provider: FileProvider) {
        let desc = "Enumerating files list in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.contentsOfDirectory(path: "/") { (files, error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            XCTAssertGreaterThan(files.count, 0, "list is empty")
            let testFolder = files.filter({ $0.name == self.testFolderName }).first
            XCTAssertNotNil(testFolder, "Test folder didn't listed")
            guard testFolder != nil else { return }
            XCTAssertTrue(testFolder!.isDirectory, "Test entry is not a folder")
            XCTAssertLessThanOrEqual(testFolder!.size, 0, "folder size is not -1")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testAttributesOfFile(_ provider: FileProvider, filePath: String) {
        let desc = "Attrubutes of file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.attributesOfItem(path: filePath) { (fileObject, error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            XCTAssertNotNil(fileObject, "file '\(filePath)' didn't exist")
            guard fileObject != nil else { return }
            XCTAssertEqual(fileObject!.path, filePath, "file path is different from '\(filePath)'")
            XCTAssertEqual(fileObject!.type, URLFileResourceType.regular, "file '\(filePath)' is not a regular file")
            XCTAssertGreaterThan(fileObject!.size, 0, "file '\(filePath)' is empty")
            XCTAssertNotNil(fileObject!.modifiedDate, "file '\(filePath)' has no modification date")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testCreateFile(_ provider: FileProvider, filePath: String) {
        let desc = "Creating file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        let data = sampleText.data(using: .ascii)
        provider.writeContents(path: filePath, contents: data, overwrite: true) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout * 3)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testContentsFile(_ provider: FileProvider, filePath: String, hasSampleText: Bool = true) {
        let desc = "Reading file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.contents(path: filePath) { (data, error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            XCTAssertNotNil(data, "no data for test file")
            if data != nil && hasSampleText {
                let str = String(data: data!, encoding: .ascii)
                XCTAssertNotNil(str, "test file data not readable")
                XCTAssertEqual(str, self.sampleText, "test file data didn't matched")
            }
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout * 3)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testRenameFile(_ provider: FileProvider, filePath: String, to toPath: String) {
        let desc = "Renaming file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.moveItem(path: filePath, to: toPath, overwrite: true) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testCopyFile(_ provider: FileProvider, filePath: String, to toPath: String) {
        let desc = "Copying file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.copyItem(path: filePath, to: toPath, overwrite: true) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        if provider is FTPFileProvider {
            // FTP will need to download and upload file again.
            wait(for: [expectation], timeout: timeout * 6)
        } else {
            wait(for: [expectation], timeout: timeout)
        }
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testRemoveFile(_ provider: FileProvider, filePath: String) {
        let desc = "Deleting file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.removeItem(path: filePath) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    private func randomData(size: Int = 262144) -> Data {
        var keyData = Data(count: size)
        let count = keyData.count
        let result = keyData.withUnsafeMutableBytes {
            SecRandomCopyBytes(kSecRandomDefault, count, $0)
        }
        if result == errSecSuccess {
            return keyData
        } else {
            fatalError("Problem generating random bytes")
        }
    }
    
    fileprivate func dummyFile() -> URL {
        let url = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!.appendingPathComponent("dummyfile.dat")
        
        if !FileManager.default.fileExists(atPath: url.path) {
            let data = randomData()
            try! data.write(to: url)
        }
        return url
    }
    
    fileprivate func testUploadFile(_ provider: FileProvider, filePath: String) {
        // test Upload/Download
        let desc = "Uploading file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        let dummy = dummyFile()
        provider.copyItem(localFile: dummy, to: filePath) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout * 3)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testDownloadFile(_ provider: FileProvider, filePath: String) {
        let url = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!.appendingPathComponent("downloadedfile.dat")
        let desc = "Downloading file in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.copyItem(path: filePath, toLocalURL: url) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            XCTAssertTrue(FileManager.default.fileExists(atPath: url.path), "downloaded file doesn't exist")
            let size = (try? FileManager.default.attributesOfItem(atPath: url.path))?[FileAttributeKey.size] as? Int64
            XCTAssertEqual(size, 262144, "downloaded file size is unexpected")
            XCTAssert(FileManager.default.contentsEqual(atPath: self.dummyFile().path, andPath: url.path), "downloaded data is corrupted")
            try? FileManager.default.removeItem(at: url)
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout * 3)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testStorageProperties(_ provider: FileProvider, isExpected: Bool) {
        let desc = "Querying volume in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.storageProperties { (volume) in
            if !isExpected {
                XCTAssertNotNil(volume, "volume information is nil")
                guard volume != nil else { return }
                XCTAssertGreaterThan(volume!.totalCapacity, 0, "capacity must be greater than 0")
                XCTAssertGreaterThan(volume!.availableCapacity, 0, "available capacity must be greater than 0")
                XCTAssertEqual(volume!.totalCapacity, volume!.availableCapacity + volume!.usage, "total capacity is not equal to usage + available")
            } else {
                XCTAssertNil(volume, "volume information must be nil")
            }
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testReachability(_ provider: FileProvider) {
        let desc = "Reachability of \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.isReachable { (status, error) in
            XCTAssertTrue(status, "\(provider.type) not reachable: \(error?.localizedDescription ?? "no error desc")")
            expectation.fulfill()
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testArchiving(_ provider: FileProvider) {
        let archivedData = NSKeyedArchiver.archivedData(withRootObject: provider)
        let unarchived = NSKeyedUnarchiver.unarchiveObject(with: archivedData) as? FileProvider
        XCTAssertNotNil(unarchived)
        XCTAssertEqual(unarchived?.baseURL, provider.baseURL, "archived provider is not same with original")
        XCTAssertEqual(unarchived?.credential, provider.credential, "archived provider is not same with original")
        if let provider = provider as? FileProviderBasicRemote, let unarchived_r = unarchived as? FileProviderBasicRemote {
            XCTAssertEqual(unarchived_r.useCache, provider.useCache, "archived provider is not same with original")
            XCTAssertEqual(unarchived_r.validatingCache, provider.validatingCache, "archived provider is not same with original")
        }
        if let provider = provider as? OneDriveFileProvider, let unarchived_o = unarchived as? OneDriveFileProvider {
            XCTAssertEqual(unarchived_o.route.rawValue, provider.route.rawValue, "archived provider is not same with original")
        }
    }
    
    fileprivate func testSymlink(_ provider: FileProvider & FileProviderSymbolicLink, filePath: String) {
        let desc = "Symlink in \(provider.type)"
        print("Test started: \(desc).")
        let expectation = XCTestExpectation(description: desc)
        provider.create(symbolicLink: filePath + " Link", withDestinationPath: filePath) { (error) in
            XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
            provider.destination(ofSymbolicLink: filePath + " Link", completionHandler: { (fileObject, error) in
                provider.removeItem(path: filePath + " Link", completionHandler: nil)
                XCTAssertNil(error, "\(desc) failed: \(error?.localizedDescription ?? "no error desc")")
                XCTAssertNotNil(fileObject, "file '\(filePath)' didn't exist")
                guard fileObject != nil else { return }
                XCTAssertEqual(fileObject!.path, filePath, "file path is different from '\(filePath)'")
                XCTAssertEqual(fileObject!.type, URLFileResourceType.regular, "file '\(filePath)' is not a regular file")
                expectation.fulfill()
            })
        }
        wait(for: [expectation], timeout: timeout)
        print("Test fulfilled: \(desc).")
    }
    
    fileprivate func testBasic(_ provider: FileProvider) {
        let filepath = "/test/file.txt"
        let fileurl = provider.url(of: filepath)
        let composedfilepath = provider.relativePathOf(url: fileurl)
        XCTAssertEqual(composedfilepath, "test/file.txt", "file url synthesis error")
        
        let dirpath = "/test/"
        let dirurl = provider.url(of: dirpath)
        let composeddirpath = provider.relativePathOf(url: dirurl)
        XCTAssertEqual(composeddirpath, "test", "directory url synthesis error")
        
        let rooturl1 = provider.url(of: "")
        let rooturl2 = provider.url(of: "/")
        XCTAssertEqual(rooturl1, rooturl2, "root url synthesis error")
    }
    
    fileprivate func testOperations(_ provider: FileProvider) {
        // Test file operations
        testReachability(provider)
        testCreateFolder(provider, folderName: testFolderName)
        testContentsOfDirectory(provider)
        testCreateFile(provider, filePath: textFilePath)
        testAttributesOfFile(provider, filePath: textFilePath)
        testContentsFile(provider, filePath: textFilePath)
        testRenameFile(provider, filePath: textFilePath, to: renamedFilePath)
        testCopyFile(provider, filePath: renamedFilePath, to: textFilePath)
        if let provider = provider as? FileProvider & FileProviderSymbolicLink {
            testSymlink(provider, filePath: textFilePath)
        }
        testRemoveFile(provider, filePath: textFilePath)
        
        // TODO: Test search
        // TODO: Test provider delegate
        
        // Test upload/download
        testUploadFile(provider, filePath: uploadFilePath)
        testDownloadFile(provider, filePath: uploadFilePath)
    }
    
    func fileproviderSucceed(_ fileProvider: FileProviderOperations, operation: FileOperationType) {
        return
    }
    
    func fileproviderFailed(_ fileProvider: FileProviderOperations, operation: FileOperationType, error: Error) {
        return
    }
    
    func fileproviderProgress(_ fileProvider: FileProviderOperations, operation: FileOperationType, progress: Float) {
        switch operation {
        case .copy(source: let source, destination: let dest) where dest.hasPrefix("file://"):
            print("Downloading \(source) to \((dest as NSString).lastPathComponent): \(progress * 100) completed.")
        case .copy(source: let source, destination: let dest) where source.hasPrefix("file://"):
            print("Uploading \((source as NSString).lastPathComponent) to \(dest): \(progress * 100) completed.")
        case .copy(source: let source, destination: let dest):
            print("Copy \(source) to \(dest): \(progress * 100) completed.")
        default:
            break
        }
        return
    }
}
*/

import XCTest
@testable import FilesProvider

/// HTTP 다운로드 완료 핸들러가 중복 호출되지 않는지 확인하는 테스트
/// - Note: 실제 서버가 필요 없도록, 연결이 거부되는 로컬 주소를 사용한다
class HTTPDownloadCompletionTests: XCTestCase {

    /// 연결이 거부되는 주소의 WebDAV provider 생성
    private func makeUnreachableProvider() throws -> WebDAVFileProvider {
        let unreachableURL = try XCTUnwrap(URL(string: "http://127.0.0.1:9/"))
        return try XCTUnwrap(WebDAVFileProvider(baseURL: unreachableURL, credential: nil, credentialType: .basic))
    }

    /// 작업 등록이 거부된 상태에서 `contents(path:offset:length:)` 를 호출해도, 완료 핸들러는 한 번만 호출되어야 한다
    func testCompletionIsCalledOnceWhenSerialWorkRegistrationIsRefused() throws {
        let provider = try makeUnreachableProvider()
        // 작업 등록 차단
        provider.allowRegisterSerialWork = false

        let lock = NSLock()
        var completionCallCount = 0
        let firstCompletion = expectation(description: "첫 번째 완료 핸들러 호출")

        _ = provider.contents(path: "sample.bin", offset: 0, length: 16) { _, _ in
            lock.lock()
            completionCallCount += 1
            let currentCallCount = completionCallCount
            lock.unlock()
            if currentCallCount == 1 {
                firstCompletion.fulfill()
            }
        }
        wait(for: [firstCompletion], timeout: 5)

        // 등록이 거부된 작업이 뒤늦게 완료 핸들러를 다시 호출하는지 일정 시간 대기
        let lateCompletionWindow = expectation(description: "뒤늦은 완료 핸들러 호출 대기")
        lateCompletionWindow.isInverted = true
        wait(for: [lateCompletionWindow], timeout: 5)

        lock.lock()
        let finalCallCount = completionCallCount
        lock.unlock()
        XCTAssertEqual(finalCallCount, 1, "완료 핸들러가 \(finalCallCount)번 호출됨")
    }

    /// `onceOnly` 로 감싼 핸들러는 여러 쓰레드에서 동시에 호출해도 최초 1회만 실행되어야 한다
    func testOnceOnlyRunsHandlerExactlyOnce() {
        let lock = NSLock()
        var handlerCallCount = 0
        var receivedErrors = [Error?]()
        let completionHandler = HTTPFileProvider.onceOnly { error in
            lock.lock()
            handlerCallCount += 1
            receivedErrors.append(error)
            lock.unlock()
        }

        // 순차 호출: 첫 번째 값만 전달되어야 한다
        completionHandler(nil)
        completionHandler(URLError(.cancelled))
        // 동시 호출
        DispatchQueue.concurrentPerform(iterations: 1000) { _ in
            completionHandler(URLError(.unknown))
        }

        XCTAssertEqual(handlerCallCount, 1)
        XCTAssertEqual(receivedErrors.count, 1)
        XCTAssertNil(receivedErrors.first ?? URLError(.unknown))
    }
}
