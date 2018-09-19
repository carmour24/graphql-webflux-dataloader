package com.yg.gqlwfdl

import com.yg.gqlwfdl.services.CompositeEntityCreationListener
import com.yg.gqlwfdl.services.DataLoaderPrimerEntityCreationListener
import com.yg.gqlwfdl.services.Entity
import com.yg.gqlwfdl.services.UnitOfWorkAwareEntityCreationListener
import com.yg.gqlwfdl.unitofwork.UnitOfWork
import graphql.ExecutionInput
import graphql.schema.DataFetchingEnvironment
import org.dataloader.DataLoaderRegistry

/**
 * An object representing the context of a single HTTP request (e.g. a GraphQL request). Passed from the GraphQL
 * entry points to the resolvers by storing it in the [ExecutionInput.context]. This allows the resolvers to then
 * retrieve it by calling the [DataFetchingEnvironment.getContext] method (though an extension method exists to get this
 * more directly: see [DataFetchingEnvironment.requestContext]
 *
 * The primary use of this object is to provide access to the [DataLoaderRegistry] so that the resolvers have access to
 * the (request-scoped) data loaders.
 *
 * @property dataLoaderRegistry The registry of data loaders which provides access to the data loaders.
 */
class RequestContext(val dataLoaderRegistry: DataLoaderRegistry, val unitOfWork: UnitOfWork) {

    // TODO: this object is a "newable" (rather than "injectable") but it constructs the entity creation listener, which
    // is an "injectable". Also it doesn't implement an interface. So basically this object can't really be unit tested
    // (or, more specifically, the entityCreationListener can't be unit tested) so maybe revisit this a bit.

    /**
     * The object to be informed when [Entity] objects are created. Responds by priming the data loaders (i.e. pre-
     * caching them with the entities, so that subsequent requests for those entities can use the cached values rather
     * that querying for them again).
     */
    val entityCreationListener = CompositeEntityCreationListener(
            DataLoaderPrimerEntityCreationListener(this),
            UnitOfWorkAwareEntityCreationListener(this)
    )
}