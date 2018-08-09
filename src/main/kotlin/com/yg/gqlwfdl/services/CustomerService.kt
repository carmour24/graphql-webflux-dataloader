package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.CustomerRepository
import graphql.schema.DataFetchingEnvironment
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to customers. Communicates with the data access layer to get the data
 * from the database, and exposes it to callers using the domain model objects (specifically, [Customer]). Performs
 * all actions asynchronously.
 */
interface CustomerService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Customer]s.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findAll(env: DataFetchingEnvironment? = null): CompletableFuture<List<Customer>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Customer]s with the passed in
     * IDs.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findByIds(ids: List<Long>, env: DataFetchingEnvironment? = null): CompletableFuture<List<Customer>>
}

/**
 * Concrete implementation of [see CustomerService]
 */
@Service
class DefaultCustomerService(private val customerRepository: CustomerRepository)
    : CustomerService {

    override fun findAll(env: DataFetchingEnvironment?) = customerRepository.findAll(env)

    override fun findByIds(ids: List<Long>, env: DataFetchingEnvironment?) = customerRepository.findByIds(ids, env)
}