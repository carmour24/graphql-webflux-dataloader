package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.CompanyPartnershipRepository
import graphql.schema.DataFetchingEnvironment
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to company partnerships. Communicates with the data access layer to get
 * the data from the database, and exposes it to callers using the domain model objects (specifically,
 * [CompanyPartnership]). Performs all actions asynchronously.
 */
interface CompanyPartnershipService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [CompanyPartnership] objects.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findAll(env: DataFetchingEnvironment? = null): CompletableFuture<List<CompanyPartnership>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [CompanyPartnership] objects
     * with the passed in IDs.
     *
     * @param env The environment for the current GraphQL data fetch, if this method is called from such a context.
     */
    fun findByIds(ids: List<Long>, env: DataFetchingEnvironment? = null): CompletableFuture<List<CompanyPartnership>>
}

/**
 * Concrete implementation of [see CompanyPartnershipService]
 */
@Service
class DefaultCompanyPartnershipService(private val companyPartnershipRepository: CompanyPartnershipRepository)
    : CompanyPartnershipService {

    override fun findAll(env: DataFetchingEnvironment?) = companyPartnershipRepository.findAll(env)

    override fun findByIds(ids: List<Long>, env: DataFetchingEnvironment?) = companyPartnershipRepository.findByIds(ids, env)
}
