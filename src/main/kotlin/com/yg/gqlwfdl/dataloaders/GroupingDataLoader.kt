package com.yg.gqlwfdl.dataloaders

import java.util.concurrent.CompletionStage

/**
 * Implementation of [ContextAwareDataLoader] which fetches/caches items, then groups them by some property of theirs,
 * defined by the `keySelector` parameter, exposing the grouped items as a list.
 *
 * For example, say a Customer object has zero or more related Order objects: this data loader can be used to fetch all
 * orders for a set of customers, then expose a list of orders for each of those customers (returning null for those
 * customers which had no orders).
 *
 * Also responsible for ensuring that when items are returned from a data loader, they are returned in the exact order
 * in which they were requested (as this is part of the data loader contract).
 */
class GroupingDataLoader<K, V>(childFieldStore: ClientFieldStore,
                               keySelector: (V) -> K,
                               loader: (List<K>) -> CompletionStage<List<V>>)
    : ContextAwareDataLoader<K, List<V>>(childFieldStore,
        { keys -> loader(keys).thenApply { it.groupBy(keySelector).syncWithKeys(keys) } })

/**
 * From the receiver (a list of items which the data loader fetched by calling the `loader` function passed into the
 * constructor), returns a list of the same values, but in corresponding order to the passed in keys. This is required
 * as it's part of the DataLoader contract that values must be returned in corresponding order to the keys. If any key
 * doesn't have a corresponding item in the database, null is returned.
 *
 * @param keys The keys to synchronise the receiver with.
 */
fun <K, V> Map<K, List<V>>.syncWithKeys(keys: Iterable<K>): List<List<V>?> = keys.map { this[it] }