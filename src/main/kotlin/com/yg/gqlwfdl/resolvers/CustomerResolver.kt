package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLResolver
import com.yg.gqlwfdl.dataloaders.companyDataLoader
import com.yg.gqlwfdl.dataloaders.customerDataLoader
import com.yg.gqlwfdl.dataloaders.customerOrderDataLoader
import com.yg.gqlwfdl.dataloaders.pricingDetailsDataLoader
import com.yg.gqlwfdl.requestContext
import com.yg.gqlwfdl.services.Company
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.Order
import com.yg.gqlwfdl.services.PricingDetails
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

@Suppress("unused")
/**
 * Resolver for [Customer]s. Provides access to properties which the GraphQL schema exposes of these objects, but which
 * don't exist directly on the domain model object (Customer in this case), and need to be queried for separately. This
 * is done by delegating the work to the data loaders, so that the N+1 problem is bypassed, and the fetches can be
 * batched in one single call.
 */
class CustomerResolver : DataLoadingResolver(), GraphQLResolver<Customer> {

    /**
     * Gets a [CompletableFuture] which, when completed, will return the company for the passed in customer.
     */
    fun company(customer: Customer, env: DataFetchingEnvironment): CompletableFuture<Company> =
            prepareDataLoader(env) { env.requestContext.companyDataLoader }.load(customer.companyId)

    /**
     * Gets a [CompletableFuture] which, when completed, will return the out-of-office delegate for the passed in
     * customer.
     *
     * This might return null.
     */
    fun outOfOfficeDelegate(customer: Customer, env: DataFetchingEnvironment): CompletableFuture<Customer?> =
            if (customer.outOfOfficeDelegate == null) CompletableFuture.completedFuture(null)
            else prepareDataLoader(env) { env.requestContext.customerDataLoader }.load(customer.outOfOfficeDelegate)

    /**
     * Gets a [CompletableFuture] which, when completed, will return the pricing details for the passed in customer.
     */
    fun pricingDetails(customer: Customer, env: DataFetchingEnvironment): CompletableFuture<PricingDetails> =
            prepareDataLoader(env) { env.requestContext.pricingDetailsDataLoader }.load(customer.pricingDetailsId)

    /**
     * Gets a [CompletableFuture] which, when completed, will return the [Order]s for the passed in customer.
     */
    fun orders(customer: Customer, env: DataFetchingEnvironment): CompletableFuture<List<Order>> =
            prepareDataLoader(env) { env.requestContext.customerOrderDataLoader }.load(customer.id)
}