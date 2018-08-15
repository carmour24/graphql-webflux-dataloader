package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.CompanyRepository
import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to companies. Communicates with the data access layer to get the data
 * from the database, and exposes it to callers using the domain model objects (specifically, [Company]). Performs
 * all actions asynchronously.
 */
interface CompanyService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Company] objects.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findAll(requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Company>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Company] objects with the
     * passed in IDs.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Company>>
}

/**
 * Concrete implementation of [see CompanyService]
 */
@Service
class DefaultCompanyService(private val companyRepository: CompanyRepository) : CompanyService {

    override fun findAll(requestInfo: EntityRequestInfo?) = companyRepository.findAll(requestInfo)

    override fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            companyRepository.findByIds(ids, requestInfo)
}