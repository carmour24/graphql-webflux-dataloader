package com.yg.gqlwfdl

import com.yg.gqlwfdl.dataloaders.DataLoaderType
import com.yg.gqlwfdl.dataloaders.EntityDataLoader
import com.yg.gqlwfdl.services.DataLoaderPrimerEntityCreationListener
import com.yg.gqlwfdl.services.Entity
import graphql.ExecutionInput
import graphql.schema.DataFetchingEnvironment
import org.dataloader.DataLoaderRegistry

/**
 * An object representing the context of a single HTTP request (i.e. a GraphQL request). Passed from the GraphQL
 * entry points to the resolvers by storing it in the [ExecutionInput.context]. This allows the resolvers to then
 * retrieve it by calling the [DataFetchingEnvironment.getContext] method. The primary use of this object is to provide
 * access to the [DataLoaderRegistry] so that the resolvers have access to the (request-scoped) data loaders.
 *
 * @property dataLoaderRegistry The registry of data loaders which provides access to the data loaders.
 */
class RequestContext(private val dataLoaderRegistry: DataLoaderRegistry) {

    /**
     * Gets a data loader registered with the passed in key, in the current request context. Throws an exception if no
     * such data loader is registered.
     *
     * @param K The type of the key which the items cached by the requested data loader are stored (i.e. the type of
     * its unique identifier).
     * @param V The type of the objects cached by the requested data loader.
     * @param type The type of the data loader being requested.
     */
    fun <K, V> dataLoader(type: DataLoaderType) =
            dataLoaderRegistry.getDataLoader<K, V>(type.registryKey) as EntityDataLoader

    /**
     * The object to be informed when [Entity] objects are created. Responds by priming the data loaders (i.e. pre-
     * caching them with the entities, so that subsequent requests for those entities can use the cached values rather
     * that querying for them again).
     */
    val dataLoaderPrimerEntityCreationListener = DataLoaderPrimerEntityCreationListener(this)
}

/**
 * Gets the [RequestContext] of the current request from the receiver, which is the current [DataFetchingEnvironment].
 */
val DataFetchingEnvironment.requestContext: RequestContext
    get() = this.getContext<RequestContext>()