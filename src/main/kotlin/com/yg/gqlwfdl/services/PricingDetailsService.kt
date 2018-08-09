package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.PricingDetailsRepository
import graphql.schema.DataFetchingEnvironment
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to pricing details.  Communicates with the data access layer to get
 * the data from the database, and exposes it to callers using the domain model objects (specifically,
 * [PricingDetails]). Performs all actions asynchronously.
 */
interface PricingDetailsService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [PricingDetails] objects.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findAll(env: DataFetchingEnvironment? = null): CompletableFuture<List<PricingDetails>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [PricingDetails] objects
     * with the passed in IDs.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findByIds(ids: List<Long>, env: DataFetchingEnvironment? = null): CompletableFuture<List<PricingDetails>>
}

/**
 * Concrete implementation of [see PricingDetailsService]
 */
@Service
class DefaultPricingDetailsService(private val pricingDetailsRepository: PricingDetailsRepository)
    : PricingDetailsService {

    override fun findAll(env: DataFetchingEnvironment?) = pricingDetailsRepository.findAll(env)

    override fun findByIds(ids: List<Long>, env: DataFetchingEnvironment?) = pricingDetailsRepository.findByIds(ids, env)
}
