package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

/**
 * Defines a repository responsible for working with [Entity] objects.
 */
interface EntityRepository<TEntity : Entity<TId>, TId : Any> {

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items in the
     * system.
     *
     * @param env The GraphQL data fetching environment from which this call was made, if it was made from that context.
     * Can be null if not working within a GraphQL context.
     */
    fun findAll(env: DataFetchingEnvironment? = null): CompletableFuture<List<TEntity>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items which have
     * the passed in IDs.
     *
     * @param ids The IDs of the items to be found.
     * @param env The GraphQL data fetching environment from which this call was made, if it was made from that context.
     * Can be null if not working within a GraphQL context.
     */
    fun findByIds(ids: List<TId>, env: DataFetchingEnvironment? = null): CompletableFuture<List<TEntity>>
}