package com.yg.gqlwfdl.dataloaders

import java.util.concurrent.CompletionStage

/**
 * Implementation of [ContextAwareDataLoader] which simply fetches/caches items, caching them by some unique ID property,
 *
 * Also responsible for ensuring that when items are returned from a data loader, they are returned in the exact order
 * in which they were requested (as this is part of the data loader contract).
 */
class SimpleDataLoader<K, V>(childFieldStore: ClientFieldStore,
                             keySelector: (V) -> K,
                             loader: (List<K>) -> CompletionStage<List<V>>)
    : ContextAwareDataLoader<K, V>(childFieldStore,
        { keys -> loader(keys).thenApply { it.syncWithKeys(keys, keySelector) } })

/**
 * From the receiver (a list of items which the data loader fetched by calling the `loader` function passed into the
 * constructor), returns a list of the same values, but in corresponding order to the passed in keys. This is required
 * as it's part of the DataLoader contract that values must be returned in corresponding order to the keys. If any key
 * doesn't have a corresponding item in the database, null is returned.
 *
 * @param keys The keys to synchronise the receiver with.
 * @param keySelector A function which takes in an object of type [V] (e.g. some sort of entity) and returns its ID,
 * which is the key, of type [K], against which it's stored in the data loader's cache.
 */
fun <K, V> Iterable<V>.syncWithKeys(keys: Iterable<K>, keySelector: (V) -> K): List<V?> {
    val objectsMap = this.associateBy(keySelector)
    return keys.map { key -> objectsMap[key] }
}