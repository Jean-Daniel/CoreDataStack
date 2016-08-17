//
//  NSPersistentStoreCoordinator+Extensions.swift
//  CoreDataStack
//
//  Created by Robert Edwards on 5/8/15.
//  Copyright (c) 2015 Big Nerd Ranch. All rights reserved.
//

import CoreData

public extension NSPersistentStoreCoordinator {

  /**
   Default persistent store options used for the `SQLite` backed `NSPersistentStoreCoordinator`
   */
  // Note: the obj generated code fails to compile. Workaround the issue by not exporting that propery.
  public static var stockSQLiteStoreOptions: [String: Any] {
    return [
             NSMigratePersistentStoresAutomaticallyOption: true,
             NSInferMappingModelAutomaticallyOption: true,
             NSSQLitePragmasOption: ["journal_mode": "WAL"]
    ]
  }

  /**
   Asynchronously creates an `NSPersistentStoreCoordinator` and adds a `SQLite` based store.

   - parameter managedObjectModel: The `NSManagedObjectModel` describing the data model.
   - parameter storeFileURL: The URL where the SQLite store file will reside.
   - parameter completion: A completion closure with a `CoordinatorResult` that will be executed following the `NSPersistentStore` being added to the `NSPersistentStoreCoordinator`.
   */
  public class func setupSQLiteBackedCoordinator(_ managedObjectModel: NSManagedObjectModel,
                                                 storeFileURL: URL,
                                                 completion: @escaping (CoreDataStackResult<NSPersistentStoreCoordinator>) -> Void) {
    let backgroundQueue : DispatchQueue = DispatchQueue.global(qos: .background)
    backgroundQueue.async {
      do {
        let coordinator = NSPersistentStoreCoordinator(managedObjectModel: managedObjectModel)
        try coordinator.addPersistentStore(ofType: NSSQLiteStoreType,
                                           configurationName: nil,
                                           at: storeFileURL,
                                           options: stockSQLiteStoreOptions)
        completion(.success(coordinator))
      } catch {
        completion(.failure(error as NSError))
      }
    }
  }
}
