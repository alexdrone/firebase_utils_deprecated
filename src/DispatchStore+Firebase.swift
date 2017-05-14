/**
 * Copyright (c) 2017-present Alex Usbergo (github/alexdrone).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import Foundation
import DispatchStore

import Firebase
import FirebaseDatabase

import Wrap
import Unbox

// MARK: Action protocol

/** Specialization for the 'ActionType'. */
public protocol FirebaseActionType: ActionType {

  /** Action dispatched whenever there's a remote change from the server. */
  static var actionForRemoteDatabaseChange: FirebaseActionType { get }

  /** Whether this action is the one marked for remote synchronization. */
  var isActionForRemoteDatabaseChange: Bool { get }
}

// MARK: - Provider

public protocol FirebaseBackedStoreProviderProviderType: class {

  /** Path to the Firebase resource. */
  var path: String { get }

  /** A reference to the dispatch store. */
  var storeRef: StoreType { get }

  /** The firebase handle. */
  var ref: FIRDatabaseReference? { get }
}

/** A Firebase-backed store automatically synchronize the store at the location specificed
 *  in the path.
 *  Whenever the 'path' location changes remotely, the 'remoteDatabaseChange' action is
 *  dispatched and the new data from the store injected.
 *  Similiarly whenever the store changes locally, the diffs are computed end the commited to the 
 *  remote store.
 */
open class FirebaseBackedStoreProvider<S: FirebaseStateType,
                                       A: FirebaseActionType>:
                                          FirebaseBackedStoreProviderProviderType {
  public let path: String
  public var ref: FIRDatabaseReference? = nil

  /** A reference to the dispatch store. */
  public let store: Store<S, A>
  public var storeRef: StoreType {
    return self.store
  }

  public init(path: String, reducer: FirebaseReducer<S, A>) {
    self.path = path
    self.store = Store<S, A>(identifier: path, reducer: reducer)
    Dispatcher.default.register(middleware: FirebaseSynchronizer(firebaseStore: self))
    self.startRemoteObservation()
    reducer.ref = self.ref
  }

  public func startRemoteObservation() {
    self.ref = FIRDatabase.database().reference().child(path)
    self.ref?.observe(FIRDataEventType.value, with: { (snapshot) in
      guard let dictianary = snapshot.value as? [String: Any] else {
        return
      }
      let state: S = StateEncoder.decode(dictionary: dictianary)
      self.store.inject(state: state, action: A.actionForRemoteDatabaseChange)
      Dispatcher.default.dispatch(action: A.actionForRemoteDatabaseChange)
    })
  }

  public func stopRemoteObservation() {
    self.ref = nil
  }
}

// MARK: - Reducer

public class FirebaseReducer<S: FirebaseStateType, A: FirebaseActionType>: Reducer<S, A> {

  /** The firebase handler. */
  var ref: FIRDatabaseReference?
}

// MARK: - Serialization

/** Firebase-backed store's states should be encodable and decodable. 
  * For the time being 'Unbox' is used as json-parser.
  */
public protocol FirebaseStateType: StateType, Unboxable { }

public extension FirebaseStateType {

  /** Infer the state target. */
  public func decoder() -> ([String: Any]) -> StateType {
     return { dictionary in
      do {
        let state: Self = try unbox(dictionary: dictionary)
        return state
      } catch {
        return Self()
      }
    }
  }
}

// MARK: State Encoder

public class StateEncoder {

  /** Wraps the state into a dictionary. */
  public static func encode(state: StateType) -> [String: Any] {
    do {
      return try wrap(state)
    } catch {
      return [:]
    }
  }

  /** Unmarshal the state from the dictionary */
  public static func decode<S: FirebaseStateType>(dictionary: [String: Any]) -> S {
    do {
      return try unbox(dictionary: dictionary)
    } catch {
      return S()
    }
  }

  /** Flatten down the dictionary into a map from 'path' to value. */
  public static func merge(encodedState: [String: Any]) -> [String: Any] {
    func flatten(path: String, dictionary: [String: Any], result: inout [String: Any]) {
      let formattedPath = path.isEmpty ? "" : "\(path)/"
      for (key, value) in dictionary {
        if let nestedDictionary = value as? [String: Any] {
          flatten(path: "\(formattedPath)\(key)",
                  dictionary: nestedDictionary,
                  result: &result)
        } else {
          result["\(formattedPath)\(key)"] = value
        }
      }
    }
    var result: [String: Any] = [:]
    flatten(path: "", dictionary: encodedState, result: &result)
    return result
  }
}

// MARK: - Syncronization Middleware

open class FirebaseSynchronizer<S: FirebaseStateType, A: FirebaseActionType>: MiddlewareType {

  // A reference to the associated store.
  private weak var firebaseStore: FirebaseBackedStoreProvider<S, A>?

  // The current encoded state the store.
  private var currentFlattenState: [String: Any] = [:]

  // Syncronizes the access to the state object.
  private let lock = NSRecursiveLock()

  init(firebaseStore: FirebaseBackedStoreProvider<S, A>) {
    self.firebaseStore = firebaseStore
  }

  /** An action is about to be dispatched. */
  public func willDispatch(transaction: String, action: ActionType, in store: StoreType) {
    // Nothing to do.
  }

  /** An action just got dispatched. */
  public func didDispatch(transaction: String, action: ActionType, in store: StoreType) {
    guard let store = store as? Store<S, A>, store === self.firebaseStore?.storeRef,
          let action = action as? A else {
      return
    }

    self.lock.lock()

    // Encode the new state into a flatten map.
    // e.g
    // "items/0/name": "Foo",
    // "location: "Stockholm"
    let new = StateEncoder.merge(encodedState: StateEncoder.encode(state: store.state))

    // Set the current state to the new encoded state.
    let old = self.currentFlattenState
    self.currentFlattenState = new

    // No need to upload the changes if the changes comes from a remote change.
    guard !action.isActionForRemoteDatabaseChange else {
      self.lock.unlock()
      return
    }

    // Computes the diff between the old and the new state.
    var diff: [String: Any] = [:]
    for (key, value) in new {
      if old[key] == nil || new[key].debugDescription != old[key].debugDescription {
        diff[key] = value
      }
    }

    // Pushes the values.
    print("⤷ \(self.firebaseStore?.path ?? "n/a").push \(diff.count): \(diff)…")
    self.firebaseStore?.ref?.updateChildValues(diff)

    self.lock.unlock()
  }
}

//MARK: - Uuid

/** Shorthand to PushID's 'make' function. */
public func makePushID() -> String {
  return PushID.default.make()
}

/** ID generator that creates 20-character string identifiers with the following properties:
 * 1. They're based on timestamp so that they sort *after* any existing ids.
 * 2. They contain 72-bits of random data after the timestamp so that IDs won't collide with
 *    other clients' IDs.
 * 3. They sort *lexicographically* (so the timestamp is converted to characters that will
 *    sort properly).
 * 4. They're monotonically increasing. Even if you generate more than one in the same timestamp,
 *    the latter ones will sort after the former ones.  We do this by using the previous random bits
 *    but "incrementing" them by 1 (only in the case of a timestamp collision).
 */
public class PushID {

  // MARK: Static constants

  // Modeled after base64 web-safe chars, but ordered by ASCII.
  private static let ascChars = Array(
    "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz".characters)
  private static let descChars = Array(ascChars.reversed())

  public static let `default` = PushID()

  // MARK: State

  // Timestamp of last push, used to prevent local collisions if you push twice in one ms.
  private var lastPushTime: UInt64 = 0

  // We generate 72-bits of randomness which get turned into 12 characters and appended to the
  // timestamp to prevent collisions with other clients.  We store the last characters we
  // generated because in the event of a collision, we'll use those same characters except
  // "incremented" by one.
  private var lastRandChars = Array<Int>(repeating: 0, count: 12)

  // For testability purposes.
  private let dateProvider: (Void) -> Date

  // Ensure the generator synchronization.
  private let lock = SpinLock()

  public init(dateProvider: @escaping (Void) -> Date = { Date() }) {
    self.dateProvider = dateProvider
  }

  /** Generate a new push UUID. */
  public func make(ascending: Bool = true) -> String {
    let pushChars = ascending ? PushID.ascChars : PushID.descChars
    precondition(pushChars.count > 0)
    var timeStampChars = Array<Character>(repeating: pushChars.first!, count: 8)

    self.lock.lock()

    var now = UInt64(self.dateProvider().timeIntervalSince1970 * 1000)
    let duplicateTime = (now == self.lastPushTime)

    self.lastPushTime = now

    for i in stride(from: 7, to: 0, by: -1) {
      timeStampChars[i] = pushChars[Int(now % 64)]
      now >>= 6
    }

    assert(now == 0, "The whole timestamp should be now converted.")
    var id = String(timeStampChars)

    if !duplicateTime {
      for i in 0..<12 {
        self.lastRandChars[i] = Int(64 * Double(arc4random()) / Double(UInt32.max))
      }
    } else {
      // If the timestamp hasn't changed since last push, use the same random number,
      // except incremented by 1.
      var index: Int = 0
      for i in stride(from: 11, to: 0, by: -1) {
        index = i
        guard self.lastRandChars[i] == 63 else { break }
        self.lastRandChars[i] = 0
      }
      self.lastRandChars[index] += 1
    }

    // Appends the random characters.
    for i in 0..<12 {
      id.append(pushChars[self.lastRandChars[i]])
    }
    assert(id.lengthOfBytes(using: .utf8) == 20, "The id lenght should be 20.")

    self.lock.unlock()
    return id
  }
}

// MARK: - Spinlock implementation

public final class SpinLock {
  private var spin = OS_SPINLOCK_INIT
  private var unfair = os_unfair_lock_s()

  /** Locks a spinlock. Although the lock operation spins, it employs various strategies to back
   *  off if the lock is held. */
  fileprivate func lock() {
    if #available(iOS 10, *) {
      os_unfair_lock_lock(&unfair)
    } else {
      OSSpinLockLock(&spin)
    }
  }

  /** Unlocks a spinlock. */
  fileprivate func unlock() {
    if #available(iOS 10, *) {
      os_unfair_lock_unlock(&unfair)
    } else {
      OSSpinLockUnlock(&spin)
    }
  }
}

