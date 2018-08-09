package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.CompanyPartnershipRepository
import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
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
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findAll(requestInfo: EntityRequestInfo?): CompletableFuture<List<CompanyPartnership>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [CompanyPartnership] objects
     * with the passed in IDs.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<CompanyPartnership>>
}

/**
 * Concrete implementation of [see CompanyPartnershipService]
 */
@Service
class DefaultCompanyPartnershipService(private val companyPartnershipRepository: CompanyPartnershipRepository)
    : CompanyPartnershipService {

    override fun findAll(requestInfo: EntityRequestInfo?) = companyPartnershipRepository.findAll(requestInfo)

    override fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            companyPartnershipRepository.findByIds(ids, requestInfo)
}
