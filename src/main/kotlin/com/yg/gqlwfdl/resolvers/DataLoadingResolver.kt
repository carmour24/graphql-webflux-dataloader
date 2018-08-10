package com.yg.gqlwfdl.resolvers

import com.yg.gqlwfdl.dataloaders.ClientFieldStore
import com.yg.gqlwfdl.dataloaders.EntityDataLoader
import com.yg.gqlwfdl.getChildClientFields
import com.yg.gqlwfdl.services.Entity
import graphql.schema.DataFetchingEnvironment

/**
 * Abstract base class for all resolvers for domain level objects. Provides access to properties which the GraphQL
 * schema exposes of these objects, but which don't exist directly on the domain model object, and need to be queried
 * for separately. This is done by delegating the work to the data loader, so that the N+1 problem is bypassed, and the
 * fetches can be batches in one single call.
 */
abstract class DataLoadingResolver {

    /**
     * Prepares the data loader for use by this resolver, based on the passed in environment.
     *
     * @param dataLoaderCreator A function which will create the data loader
     * @param env The current [DataFetchingEnvironment], containing the field which is currently being populated, and
     * which caused this loader to be called. The children of this field are added to the returned [EntityDataLoader]'s
     * [childFieldStore][EntityDataLoader.childFieldStore]'s [fields][ClientFieldStore.fields].
     */
    protected fun <TId, TEntity : Entity<TId>> prepareDataLoader(
            env: DataFetchingEnvironment, dataLoaderCreator: () -> EntityDataLoader<TId, TEntity>)
            : EntityDataLoader<TId, TEntity> {

        return dataLoaderCreator().also { it.childFieldStore.addFields(env.field.getChildClientFields()) }
    }
}