package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import java.util.concurrent.CompletableFuture

/**
 * Defines a repository responsible for working with [Entity] objects.
 */
interface EntityRepository<TEntity: Entity<TId>, TId : Any> {

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items in the
     * system.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findAll(requestInfo: EntityRequestInfo? = null): CompletableFuture<List<TEntity>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items which have
     * the passed in IDs.
     *
     * @param ids The IDs of the items to be found.
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByIds(ids: List<TId>, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<TEntity>>

    /**
     * Inserts a [TEntity] to the repository. Returns a [CompletableFuture] object which is resolved when the insert
     * is completed.
     */
    fun insert(entity: TEntity): CompletableFuture<TId>

    /**
     * Inserts a [List] of entities to the repository. Returns a [List] of [CompletableFuture] objects which are
     * each resolved when their respective insert is completed.
     */
//    fun insert(entities: List<TEntity>): List<CompletableFuture<Long>>
}