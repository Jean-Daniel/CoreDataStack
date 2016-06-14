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
public typealias CoreDataStackSetupCallback = (CoreDataStackResult<CoreDataStack>) -> Void
public typealias CoreDataStackStoreResetCallback = (NSError?) -> Void
public typealias CoreDataStackBatchMOCCallback = (CoreDataStackResult<NSManagedObjectContext>) -> Void

// MARK: - Error Handling

/**
 Three layer Core Data stack comprised of:

 * A primary background queue context with an `NSPersistentStoreCoordinator`
 * A main queue context that is a child of the primary queue
 * A method for spawning many background worker contexts that are children of the main queue context

 Calling `save()` on any `NSMangedObjectContext` belonging to the stack will automatically bubble the changes all the way to the `NSPersistentStore`
 */
public final class CoreDataStack {

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
      managedObjectContext.name = "Primary Context (Main Queue)"

      NotificationCenter.default().addObserver(self,
                                                 selector: #selector(CoreDataStack.stackMemberContextDidSaveNotification(_:)),
                                                 name: NSNotification.Name.NSManagedObjectContextDidSave,
                                                 object: managedObjectContext)
    }
    // Always create the main-queue ManagedObjectContext on the main queue.
    if !Thread.isMainThread() {
      DispatchQueue.main.sync {
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
                                          in bundle: Bundle = Bundle.main(),
                                          at desiredStoreURL: URL? = nil,
                                          callbackQueue: DispatchQueue? = nil,
                                          callback: CoreDataStackSetupCallback) {

    let model = bundle.managedObjectModel(name: modelName)
    let storeFileURL = desiredStoreURL ?? URL(string: "\(modelName).sqlite", relativeTo: documentsDirectory)!
    do {
      try createDirectoryIfNecessary(at: storeFileURL)
    } catch {
      callback(.failure(error as NSError))
      return
    }

    let backgroundQueue : DispatchQueue = DispatchQueue.global(attributes: [.qosBackground])
    let callbackQueue: DispatchQueue = callbackQueue ?? backgroundQueue
    NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(
      model,
      storeFileURL: storeFileURL) { coordinatorResult in
        switch coordinatorResult {
        case .success(let coordinator):
          let stack = CoreDataStack(modelName : modelName,
                                    in: bundle,
                                    persistentStoreCoordinator: coordinator,
                                    storeType: .sqlite(at: storeFileURL))
          callbackQueue.async {
            callback(.success(stack))
          }
        case .failure(let error):
          callbackQueue.async {
            callback(.failure(error))
          }
        }
    }
  }

  private static func createDirectoryIfNecessary(at url: URL) throws {
    let fileManager = FileManager.default()
    let directory = try url.deletingLastPathComponent()
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
                                            in bundle: Bundle = Bundle.main()) throws -> CoreDataStack {
    let model = bundle.managedObjectModel(name: modelName)
    let coordinator = NSPersistentStoreCoordinator(managedObjectModel: model)
    try coordinator.addPersistentStore(ofType: NSInMemoryStoreType, configurationName: nil, at: nil, options: nil)
    let stack = CoreDataStack(modelName: modelName, in: bundle, persistentStoreCoordinator: coordinator, storeType: .inMemory)
    return stack
  }

  // MARK: - Private Implementation

  private enum StoreType {
    case inMemory
    case sqlite(at: URL)
  }

  private let managedObjectModelName: String
  private let storeType: StoreType
  private let bundle: Bundle
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

  private init(modelName: String, in bundle: Bundle, persistentStoreCoordinator: NSPersistentStoreCoordinator, storeType: StoreType) {
    self.bundle = bundle
    self.storeType = storeType
    managedObjectModelName = modelName

    self.persistentStoreCoordinator = persistentStoreCoordinator
    privateQueueContext.persistentStoreCoordinator = persistentStoreCoordinator
  }

  deinit {
    NotificationCenter.default().removeObserver(self)
  }

  private let saveBubbleDispatchGroup : DispatchGroup = DispatchGroup()
}

public enum CoreDataStackResult<T> {
  case success(T)
  case failure(NSError)
}

public extension CoreDataStack {
  /**
   This function resets the `NSPersistentStore` connected to the `NSPersistentStoreCoordinator`.
   For `SQLite` based stacks, this function will also remove the `SQLite` store from disk.

   - parameter callbackQueue: Optional GCD queue that will be used to dispatch your callback closure. Defaults to background queue used to create the stack.
   - parameter resetCallback: A callback with a `Success` or an `ErrorProtocol` value with the error
   */
  public func resetStore(_ callbackQueue: DispatchQueue? = nil, resetCallback: CoreDataStackStoreResetCallback) {
    let backgroundQueue : DispatchQueue = DispatchQueue.global(attributes: [.qosBackground])
    let callbackQueue: DispatchQueue = callbackQueue ?? backgroundQueue
    self.saveBubbleDispatchGroup.notify(queue: backgroundQueue) {
      switch self.storeType {
      case .inMemory:
        do {
          guard let store = self.persistentStoreCoordinator.persistentStores.first else {
            resetCallback(NSError(domain: NSCocoaErrorDomain, code: NSCoreDataError))
            break
          }
          try self.persistentStoreCoordinator.performAndWaitOrThrow {
            try self.persistentStoreCoordinator.remove(store)
            try self.persistentStoreCoordinator.addPersistentStore(ofType: NSInMemoryStoreType, configurationName: nil, at: nil, options: nil)
          }
          callbackQueue.async() {
            resetCallback(nil)
          }
        } catch {
          callbackQueue.async() {
            resetCallback(error as NSError)
          }
        }
        break

      case .sqlite(let storeURL):
        let coordinator = self.persistentStoreCoordinator
        let mom = self.managedObjectModel

        do {
          try coordinator.destroyPersistentStore(at: storeURL, ofType: NSSQLiteStoreType, options: nil)
        } catch {
          callbackQueue.async() {
            resetCallback(error as NSError)
          }
          return
        }

        // Setup a new stack
        NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(mom, storeFileURL: storeURL) { result in
          switch result {
          case .success (let coordinator):
            self.persistentStoreCoordinator = coordinator
            callbackQueue.async() {
              resetCallback(nil)
            }

          case .failure (let error):
            callbackQueue.async() {
              resetCallback(error)
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
  public func newChildContext(_ concurrencyType: NSManagedObjectContextConcurrencyType = .privateQueueConcurrencyType,
                              name: String? = "Main Queue Context Child") -> NSManagedObjectContext {
    if concurrencyType == .mainQueueConcurrencyType && !Thread.isMainThread() {
      preconditionFailure("Main thread MOCs must be created on the main thread")
    }

    let moc = NSManagedObjectContext(concurrencyType: concurrencyType)
    moc.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
    moc.parent = mainQueueContext
    moc.name = name

    NotificationCenter.default().addObserver(self,
                                               selector: #selector(stackMemberContextDidSaveNotification(_:)),
                                               name: NSNotification.Name.NSManagedObjectContextDidSave,
                                               object: moc)
    return moc
  }

  /**
   Creates a new background `NSManagedObjectContext` connected to
   a discrete `NSPersistentStoreCoordinator` created with the same store used by the stack in construction.

   - parameter callbackQueue: Optional GCD queue that will be used to dispatch your callback closure. Defaults to background queue used to create the stack.
   - parameter setupCallback: A callback with either the new `NSManagedObjectContext` or an `ErrorProtocol` value with the error
   */
  public func newBatchOperationContext(callbackQueue: DispatchQueue? = nil, setupCallback: CoreDataStackBatchMOCCallback) {
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
        setupCallback(.failure(error as NSError))
      }
    case .sqlite(let storeURL):
      let backgroundQueue : DispatchQueue = DispatchQueue.global(attributes: [.qosBackground])
      let callbackQueue: DispatchQueue = callbackQueue ?? backgroundQueue
      NSPersistentStoreCoordinator.setupSQLiteBackedCoordinator(managedObjectModel, storeFileURL: storeURL) { result in
        switch result {
        case .success(let coordinator):
          moc.persistentStoreCoordinator = coordinator
          callbackQueue.async() {
            setupCallback(.success(moc))
          }
        case .failure(let error):
          callbackQueue.async() {
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

    saveBubbleDispatchGroup.enter()
    parentContext.save { _ in
      self.saveBubbleDispatchGroup.leave()
    }
  }
}

private extension CoreDataStack {
  private static var documentsDirectory: URL {
    get {
      let urls = FileManager.default().urlsForDirectory(.documentDirectory, inDomains: .userDomainMask)
      return urls.first!
    }
  }
}

private extension Bundle {
  static private let modelExtension = "momd"
  func managedObjectModel(name modelName: String) -> NSManagedObjectModel {
    guard let URL = urlForResource(modelName, withExtension: Bundle.modelExtension),
      let model = NSManagedObjectModel(contentsOf: URL) else {
        preconditionFailure("Model not found or corrupted with name: \(modelName) in bundle: \(self)")
    }
    return model
  }
}
