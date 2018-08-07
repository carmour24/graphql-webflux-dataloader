package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.services.Entity
import graphql.language.Field
import org.dataloader.BatchLoader
import org.dataloader.DataLoader
import java.util.concurrent.CompletionStage

/**
 * A (request-scoped) data loader which caches [Entity] objects.
 *
 * Also responsible for ensuring that when items are returned from a data loader, they are returned in the exact order
 * in which they were requested (as this is part of the data loader contract).
 *
 * @param TId The type of the unique ID of each object being loaded by this data loader. This value is used as the key
 * which identifies each item in the data loader's cache.
 * @param TEntity The type of the object which this data loader retrieves and caches.
 * @param loader The function which is used to retrieve a list of objects of type [TEntity] based on a list of their
 * IDs, of type [TId].
 */
class EntityDataLoader<TId, TEntity : Entity<TId>>(loader: (List<TId>) -> CompletionStage<List<TEntity>>)
    : DataLoader<TId, TEntity>(
        BatchLoader { keys ->
            loader(keys).thenApply { entities -> entities.syncWithKeys(keys) { it.id } }
        }) {

    /**
     * The GraphQL fields which were in the current requested, which led to this data loader being used. For example,
     * say the request is for a set of users, and each user has a "company" property, which is fetched using this
     * data loader: in this case, this list would include that "company" field. The child fields of that company can
     * then be interrogated to see what company-related information is requested, in case more joins need to be
     * included to fetch this data.
     */
    val sourceGraphQLFields = mutableListOf<Field>()

    /**
     * Primes that passed in entity, i.e. caches it and makes it available for subsequent usage in the current request.
     *
     * @see [DataLoader.prime]
     */
    fun prime(entity: TEntity) {
        prime(entity.id, entity)
    }
}

/**
 * From the receiver (a list of items which the data loader fetched by calling the `loader` function passed into the
 * constructor), returns a list of the same values, but in corresponding order to the passed in keys. This is required
 * as it's part of the DataLoader contract that values must be returned in corresponding order to the keys. If any key
 * doesn't have a corresponding item in the database, null is returned.
 *
 * @param keys The keys to synchronise the receiver with.
 * @param keyFetcher A function which takes in an object of type [V] (e.g. some sort of entity) and returns its ID,
 * which is the key, of type [K], against which it's stored in the data loader's cache.
 */
private fun <K, V> Iterable<V>.syncWithKeys(keys: Iterable<K>, keyFetcher: (V) -> K): List<V?> {
    val objectsMap = this.associateBy(keyFetcher)
    return keys.map { key -> objectsMap[key] }
}