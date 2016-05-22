//
//  CoreDataStack.swift
//  CoreDataSMS
//
//  Created by Robert Edwards on 12/8/14.
//  Copyright (c) 2014 Big Nerd Ranch. All rights reserved.
//

import Foundation
import CoreData

// MARK: - Action callbacks
public typealias CoreDataStackSetupCallback = (CoreDataStack.SetupResult) -> Void
public typealias CoreDataStackStoreResetCallback = (CoreDataStack.ResetResult) -> Void
public typealias CoreDataStackBatchMOCCallback = (CoreDataStack.BatchContextResult) -> Void

// MARK: - Error Handling

/**
 Three layer Core Data stack comprised of:

 * A primary background queue context with an `NSPersistentStoreCoordinator`
 * A main queue context that is a child of the primary queue
 * A method for spawning many background worker contexts that are children of the main queue context

 Calling `save()` on any `NSMangedObjectContext` belonging to the stack will automatically bubble the changes all the way to the `NSPersistentStore`
 */
public final class CoreDataStack {

  /// CoreDataStack specific ErrorProtocols
  public enum Error: ErrorProtocol {
    /// Case when an `NSPersistentStore` is not found for the supplied store URL
    case storeNotFound(at: NSURL)
    /// Case when an In-Memory store is not found
    case inMemoryStoreMissing
    /// Case when the store URL supplied to contruct function cannot be used
    case unableToCreateStore(at: NSURL)
  }

  /**
   Primary persisting background managed object context. This is the top level context that possess an
   `NSPersistentStoreCoordinator` and saves changes to disk on a background queue.

   Fetching, Inserting, Deleting or Updating managed objects should occur on a child of this context rather than directly.

   note: `NSBatchUpdateRequest` and `NSAsynchronousFetchRequest` require a context with a persistent store connected directly.
   */
  private lazy var privateQueueContext: NSManagedObjectContext = {
    return self.constructPersistingContext()
  }()
  private func constructPersistingContext() -> NSManagedObjectContext {
    let managedObjectContext = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
    managedObjectContext.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
    managedObjectContext.name = "Primary Private Queue Context (Persisting Context)"
    return managedObjectContext
  }

  /**
   The main queue context for any work that will be performed on the main queue.
   Its parent context is the primary private queue context that persist the data to disk.
   Making a `save()` call on this context will automatically trigger a save on its parent via `NSNotification`.
   */
  public private(set) lazy var mainQueueContext: NSManagedObjectContext = {
    return self.constructMainQueueContext()
  }()
  private func constructMainQueueContext() -> NSManagedObjectContext {
    var managedObjectContext: NSManagedObjectContext!
    let setup: () -> Void = {
      managedObjectContext = NSManagedObjectContext(concurrencyType: .mainQueueConcurrencyType)
      managedObjectContext.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
      managedObjectContext.parent = self.privateQueueContext

      NSNotificationCenter.default().addObserver(self,
                                                 selector: #selector(CoreDataStack.stackMemberContextDidSaveNotification(_:)),
                                                 name: NSManagedObjectContextDidSaveNotification,
                                                 object: managedObjectContext)
    }
    // Always create the main-queue ManagedObjectContext on the main queue.
    if !NSThread.isMainThread() {
      dispatch_sync(dispatch_get_main_queue()) {
        setup()
      }
    } else {
      setup()
    }
    return managedObjectContext
  }

  // MARK: - Lifecycle

  /**
   Creates a `SQLite` backed Core Data stack for a given model in the supplied `NSBundle`.

   - parameter modelName: Base name of the `XCDataModel` file.
   - parameter inBundle: NSBundle that contains the `XCDataModel`. Default value is mainBundle()
   - parameter withStoreURL: Optional URL to use for storing the `SQLite` file. Defaults to "(modelName).sqlite" in the Documents directory.
   - parameter callbackQueue: Optional GCD queue that will be used to dispatch your callback closure. Defaults to background queue used to create the stack.
   - parameter callback: The `SQLite` persistent store coordinator will be setup asynchronously. This callback will be passed either an initialized `CoreDataStack` object or an `ErrorProtocol` value.
   */
  public static func constructSQLiteStack(modelName: String,
                                          in bundle: NSBundle = NSBundle.main(),
                                          at desiredStoreURL: NSURL? = nil,
                                          callbackQueue: dispatch_queue_t? = nil,
                                          callback: CoreDataStackSetupCallback) {

    let model = bundle.managedObjectModel(name: modelName)
    let storeFileURL = desiredStoreURL ?? NSURL(string: "\(modelName).sqlite", relativeTo: documentsDirectory)!
    do {
      try createDirectoryIfNecessary(at: storeFileURL)
    } catch {
      callback(.failure(Error.unableToCreateStore(at: storeFileURL)))
      return
    }

    let backgroundQueue : dispatch_queue_t = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0)
    let callbackQueue: dispatch_queue_t = callbackQueue ?? backgroundQueue
    NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(
      managedObjectModel: model,
      storeFileURL: storeFileURL) { coordinatorResult in
        switch coordinatorResult {
        case .success(let coordinator):
          let stack = CoreDataStack(modelName : modelName,
                                    in: bundle,
                                    persistentStoreCoordinator: coordinator,
                                    storeType: .sqlite(at: storeFileURL))
          dispatch_async(callbackQueue) {
            callback(.success(stack))
          }
        case .failure(let error):
          dispatch_async(callbackQueue) {
            callback(.failure(error))
          }
        }
    }
  }

  private static func createDirectoryIfNecessary(at url: NSURL) throws {
    let fileManager = NSFileManager.default()
    guard let directory = url.deletingLastPathComponent else {
      throw Error.unableToCreateStore(at: url)
    }
    try fileManager.createDirectory(at: directory, withIntermediateDirectories: true, attributes: nil)
  }

  /**
   Creates an in-memory Core Data stack for a given model in the supplied `NSBundle`.

   This stack is configured with the same concurrency and persistence model as the `SQLite` stack, but everything is in-memory.

   - parameter modelName: Base name of the `XCDataModel` file.
   - parameter inBundle: `NSBundle` that contains the `XCDataModel`. Default value is `mainBundle()`

   - throws: Any error produced from `NSPersistentStoreCoordinator`'s `addPersistentStoreWithType`

   - returns: CoreDataStack: Newly created In-Memory `CoreDataStack`
   */
  public static func constructInMemoryStack(modelName: String,
                                            in bundle: NSBundle = NSBundle.main()) throws -> CoreDataStack {
    let model = bundle.managedObjectModel(name: modelName)
    let coordinator = NSPersistentStoreCoordinator(managedObjectModel: model)
    try coordinator.addPersistentStore(ofType: NSInMemoryStoreType, configurationName: nil, at: nil, options: nil)
    let stack = CoreDataStack(modelName: modelName, in: bundle, persistentStoreCoordinator: coordinator, storeType: .inMemory)
    return stack
  }

  // MARK: - Private Implementation

  private enum StoreType {
    case inMemory
    case sqlite(at: NSURL)
  }

  private let managedObjectModelName: String
  private let storeType: StoreType
  private let bundle: NSBundle
  private var persistentStoreCoordinator: NSPersistentStoreCoordinator {
    didSet {
      privateQueueContext = constructPersistingContext()
      privateQueueContext.persistentStoreCoordinator = persistentStoreCoordinator
      mainQueueContext = constructMainQueueContext()
    }
  }
  private var managedObjectModel: NSManagedObjectModel {
    get {
      return bundle.managedObjectModel(name: managedObjectModelName)
    }
  }

  private init(modelName: String, in bundle: NSBundle, persistentStoreCoordinator: NSPersistentStoreCoordinator, storeType: StoreType) {
    self.bundle = bundle
    self.storeType = storeType
    managedObjectModelName = modelName

    self.persistentStoreCoordinator = persistentStoreCoordinator
    privateQueueContext.persistentStoreCoordinator = persistentStoreCoordinator
  }

  deinit {
    NSNotificationCenter.default().removeObserver(self)
  }

  private let saveBubbleDispatchGroup : dispatch_group_t = dispatch_group_create()
}

public extension CoreDataStack {
  // TODO: rcedwards These will be replaced with Box/Either or something native to Swift (fingers crossed) https://github.com/bignerdranch/CoreDataStack/issues/10

  // MARK: - Operation Result Types

  /// Result containing either an instance of `NSPersistentStoreCoordinator` or `ErrorProtocol`
  public enum CoordinatorResult {
    /// A success case with associated `NSPersistentStoreCoordinator` instance
    case success(NSPersistentStoreCoordinator)
    /// A failure case with associated `ErrorProtocol` instance
    case failure(ErrorProtocol)
  }
  /// Result containing either an instance of `NSManagedObjectContext` or `ErrorProtocol`
  public enum BatchContextResult {
    /// A success case with associated `NSManagedObjectContext` instance
    case success(NSManagedObjectContext)
    /// A failure case with associated `ErrorProtocol` instance
    case failure(ErrorProtocol)
  }
  /// Result containing either an instance of `CoreDataStack` or `ErrorProtocol`
  public enum SetupResult {
    /// A success case with associated `CoreDataStack` instance
    case success(CoreDataStack)
    /// A failure case with associated `ErrorProtocol` instance
    case failure(ErrorProtocol)
  }
  /// Result of void representing `Success` or an instance of `ErrorProtocol`
  public enum SuccessResult {
    /// A success case
    case success
    /// A failure case with associated ErrorProtocol instance
    case failure(ErrorProtocol)
  }
  public typealias SaveResult = SuccessResult
  public typealias ResetResult = SuccessResult
}

public extension CoreDataStack {
  /**
   This function resets the `NSPersistentStore` connected to the `NSPersistentStoreCoordinator`.
   For `SQLite` based stacks, this function will also remove the `SQLite` store from disk.

   - parameter callbackQueue: Optional GCD queue that will be used to dispatch your callback closure. Defaults to background queue used to create the stack.
   - parameter resetCallback: A callback with a `Success` or an `ErrorProtocol` value with the error
   */
  public func resetStore(callbackQueue: dispatch_queue_t? = nil, resetCallback: CoreDataStackStoreResetCallback) {
    let backgroundQueue : dispatch_queue_t = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0)
    let callbackQueue: dispatch_queue_t = callbackQueue ?? backgroundQueue
    dispatch_group_notify(self.saveBubbleDispatchGroup, backgroundQueue) {
      switch self.storeType {
      case .inMemory:
        do {
          guard let store = self.persistentStoreCoordinator.persistentStores.first else {
            resetCallback(.failure(Error.inMemoryStoreMissing))
            break
          }
          try self.persistentStoreCoordinator.performAndWaitOrThrow {
            try self.persistentStoreCoordinator.remove(store)
            try self.persistentStoreCoordinator.addPersistentStore(ofType: NSInMemoryStoreType, configurationName: nil, at: nil, options: nil)
          }
          dispatch_async(callbackQueue) {
            resetCallback(.success)
          }
        } catch {
          dispatch_async(callbackQueue) {
            resetCallback(.failure(error))
          }
        }
        break

      case .sqlite(let storeURL):
        let coordinator = self.persistentStoreCoordinator
        let mom = self.managedObjectModel

        do {
          //          if #available(iOS 9, OSX 10.11, *) {
            try coordinator.destroyPersistentStore(at: storeURL, withType: NSSQLiteStoreType, options: nil)
//          } else {
//          guard let store = coordinator.persistentStore(for: storeURL) else {
//            let error = Error.StoreNotFoundAt(url: storeURL)
//            resetCallback(.Failure(error))
//            break
//          }
//            let fm = NSFileManager()
//            try coordinator.performAndWaitOrThrow {
//              try coordinator.remove(store)
//              try fm.removeItem(at: storeURL)
//
//              // Remove journal files if present
//              // Eat the error because different versions of SQLite might have different journal files
//              let _ = try? fm.removeItem(at: storeURL.appendingPathComponent("-shm"))
//              let _ = try? fm.removeItem(at: storeURL.appendingPathComponent("-wal"))
//            }
//          }
        } catch let resetError {
          dispatch_async(callbackQueue) {
            resetCallback(.failure(resetError))
          }
          return
        }

        // Setup a new stack
        NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(managedObjectModel: mom, storeFileURL: storeURL) { result in
          switch result {
          case .success (let coordinator):
            self.persistentStoreCoordinator = coordinator
            dispatch_async(callbackQueue) {
              resetCallback(.success)
            }

          case .failure (let error):
            dispatch_async(callbackQueue) {
              resetCallback(.failure(error))
            }
          }
        }
      }
    }
  }
}

public extension CoreDataStack {

  /**
   Returns a new `NSManagedObjectContext` as a child of the main queue context.

   Calling `save()` on this managed object context will automatically trigger a save on its parent context via `NSNotification` observing.

   - parameter concurrencyType: The NSManagedObjectContextConcurrencyType of the new context.
   **Note** this function will trap on a preconditionFailure if you attempt to create a MainQueueConcurrencyType context from a background thread.
   Default value is .PrivateQueueConcurrencyType
   - parameter name: A name for the new context for debugging purposes. Defaults to *Main Queue Context Child*

   - returns: `NSManagedObjectContext` The new worker context.
   */
  public func newChildContext(concurrencyType: NSManagedObjectContextConcurrencyType = .privateQueueConcurrencyType,
                              name: String? = "Main Queue Context Child") -> NSManagedObjectContext {
    if concurrencyType == .mainQueueConcurrencyType && !NSThread.isMainThread() {
      preconditionFailure("Main thread MOCs must be created on the main thread")
    }

    let moc = NSManagedObjectContext(concurrencyType: concurrencyType)
    moc.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
    moc.parent = mainQueueContext
    moc.name = name

    NSNotificationCenter.default().addObserver(self,
                                               selector: #selector(stackMemberContextDidSaveNotification(_:)),
                                               name: NSManagedObjectContextDidSaveNotification,
                                               object: moc)
    return moc
  }

  /**
   Creates a new background `NSManagedObjectContext` connected to
   a discrete `NSPersistentStoreCoordinator` created with the same store used by the stack in construction.

   - parameter callbackQueue: Optional GCD queue that will be used to dispatch your callback closure. Defaults to background queue used to create the stack.
   - parameter setupCallback: A callback with either the new `NSManagedObjectContext` or an `ErrorProtocol` value with the error
   */
  public func newBatchOperationContext(callbackQueue: dispatch_queue_t? = nil, setupCallback: CoreDataStackBatchMOCCallback) {
    let moc = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
    moc.mergePolicy = NSMergePolicy(merge: .mergeByPropertyObjectTrumpMergePolicyType)
    moc.name = "Batch Operation Context"

    switch storeType {
    case .inMemory:
      let coordinator = NSPersistentStoreCoordinator(managedObjectModel: managedObjectModel)
      do {
        try coordinator.addPersistentStore(ofType: NSInMemoryStoreType, configurationName: nil, at: nil, options: nil)
        moc.persistentStoreCoordinator = coordinator
        setupCallback(.success(moc))
      } catch {
        setupCallback(.failure(error))
      }
    case .sqlite(let storeURL):
      let backgroundQueue : dispatch_queue_t = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0)
      let callbackQueue: dispatch_queue_t = callbackQueue ?? backgroundQueue
      NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(managedObjectModel: managedObjectModel, storeFileURL: storeURL) { result in
        switch result {
        case .success(let coordinator):
          moc.persistentStoreCoordinator = coordinator
          dispatch_async(callbackQueue) {
            setupCallback(.success(moc))
          }
        case .failure(let error):
          dispatch_async(callbackQueue) {
            setupCallback(.failure(error))
          }
        }
      }
    }
  }
}

private extension CoreDataStack {
  @objc private func stackMemberContextDidSaveNotification(_ notification: NSNotification) {
    guard let notificationMOC = notification.object as? NSManagedObjectContext else {
      assertionFailure("Notification posted from an object other than an NSManagedObjectContext")
      return
    }
    guard let parentContext = notificationMOC.parent else {
      return
    }

    dispatch_group_enter(saveBubbleDispatchGroup)
    parentContext.save { _ in
      dispatch_group_leave(self.saveBubbleDispatchGroup)
    }
  }
}

private extension CoreDataStack {
  private static var documentsDirectory: NSURL? {
    get {
      let urls = NSFileManager.default().urlsForDirectory(.documentDirectory, inDomains: .userDomainMask)
      return urls.first
    }
  }
}

private extension NSBundle {
  static private let modelExtension = "momd"
  func managedObjectModel(name modelName: String) -> NSManagedObjectModel {
    guard let URL = urlForResource(modelName, withExtension: NSBundle.modelExtension),
      let model = NSManagedObjectModel(contentsOf: URL) else {
        preconditionFailure("Model not found or corrupted with name: \(modelName) in bundle: \(self)")
    }
    return model
  }
}
