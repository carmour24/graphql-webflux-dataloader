package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLResolver
import com.yg.gqlwfdl.dataloaders.customerDataLoader
import com.yg.gqlwfdl.dataloaders.pricingDetailsDataLoader
import com.yg.gqlwfdl.requestContext
import com.yg.gqlwfdl.services.Company
import com.yg.gqlwfdl.services.CustomerEntity
import com.yg.gqlwfdl.services.PricingDetails
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

@Suppress("unused")
/**
 * Resolver for [Company] objects. Provides access to properties which the GraphQL schema exposes of these objects, but
 * which don't exist directly on the domain model object (CustomerEntity in this case), and need to be queried for separately.
 * This is done by delegating the work to the data loaders, so that the N+1 problem is bypassed, and the fetches can be
 * batched in one single call.
 */
class CompanyResolver : DataLoadingResolver(), GraphQLResolver<Company> {

    /**
     * Gets a [CompletableFuture] which, when completed, will return the primary contact for the passed in company.
     */
    fun primaryContact(company: Company, env: DataFetchingEnvironment): CompletableFuture<CustomerEntity> =
            if (company.primaryContact == null) CompletableFuture.completedFuture(null)
            else prepareDataLoader(env) { env.requestContext.customerDataLoader }.load(company.primaryContact)

    /**
     * Gets a [CompletableFuture] which, when completed, will return the pricing details for the passed in company.
     */
    fun pricingDetails(company: Company, env: DataFetchingEnvironment): CompletableFuture<PricingDetails> =
            prepareDataLoader(env) { env.requestContext.pricingDetailsDataLoader }.load(company.pricingDetailsId)
}