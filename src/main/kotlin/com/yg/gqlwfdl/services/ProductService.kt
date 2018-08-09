package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.ProductRepository
import com.yg.gqlwfdl.withLogging
import graphql.schema.DataFetchingEnvironment
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to companies. Communicates with the data access layer to get the data
 * from the database, and exposes it to callers using the domain model objects (specifically, [Product]). Performs
 * all actions asynchronously.
 */
interface ProductService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Product] objects.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findAll(env: DataFetchingEnvironment? = null): CompletableFuture<List<Product>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Product] objects with the
     * passed in IDs.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findByIds(ids: List<Long>, env: DataFetchingEnvironment? = null): CompletableFuture<List<Product>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Product] objects with the
     * passed in IDs, along with their order counts, wrapped in an [ProductOrderCount] object.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findWithOrderCount(ids: List<Long>, env: DataFetchingEnvironment? = null): CompletableFuture<List<ProductOrderCount>>

    /**
     * Gets the top [count] best selling products.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findTopSelling(count: Int, env: DataFetchingEnvironment?): CompletableFuture<List<ProductOrderCount>>
}

/**
 * Concrete implementation of [see ProductService]
 */
@Service
class DefaultProductService(private val productRepository: ProductRepository) : ProductService {
    override fun findAll(env: DataFetchingEnvironment?) = productRepository.findAll(env)

    override fun findByIds(ids: List<Long>, env: DataFetchingEnvironment?) =
            withLogging("getting companies with IDs $ids") { productRepository.findByIds(ids, env) }

    override fun findWithOrderCount(ids: List<Long>, env: DataFetchingEnvironment?) =
            productRepository.findWithOrderCount(ids, env)

    override fun findTopSelling(count: Int, env: DataFetchingEnvironment?) = productRepository.findTopSelling(count, env)
}