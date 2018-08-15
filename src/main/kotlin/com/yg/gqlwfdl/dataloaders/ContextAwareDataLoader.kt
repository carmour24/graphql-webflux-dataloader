package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.ClientField
import com.yg.gqlwfdl.services.Entity
import org.dataloader.BatchLoader
import org.dataloader.DataLoader
import java.util.concurrent.CompletionStage

/**
 * Data loader which is aware of the context in which it was called, for example being aware of the [ClientField]s
 * requested of the items to be returned (see [childFieldStore]). Typically used to fetch and cache [Entity] objects,
 * but can be used to fetch other items instead.
 *
 * @param K The type of the unique ID of each object being loaded by this data loader. This value is used as the key
 * which identifies each item in the data loader's cache.
 * @param V The type of the object which this data loader retrieves and caches.
 * @param loader The function which is used to retrieve a list of objects of type [V] based on a list of their IDs, of
 * type [K]. Must return values in a corresponding order to the passed in keys, as that's part of the data loader contract.
 */
open class ContextAwareDataLoader<K, V>(val childFieldStore: ClientFieldStore,
                                        loader: (List<K>) -> CompletionStage<List<V?>>)
    : DataLoader<K, V>(BatchLoader(loader))