package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLResolver
import com.yg.gqlwfdl.dataloaders.customerDataLoader
import com.yg.gqlwfdl.requestContext
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.Order
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

@Suppress("unused")
/**
 * Resolver for [Order] objects. Provides access to properties which the GraphQL schema exposes of these objects, but
 * which don't exist directly on the domain model object (Order in this case), and need to be queried for separately.
 * This is done by delegating the work to the data loaders, so that the N+1 problem is bypassed, and the fetches can be
 * batched in one single call.
 */
class OrderResolver : DataLoadingResolver(), GraphQLResolver<Order> {

    /**
     * Gets a [CompletableFuture] which, when completed, will return the primary contact for the passed in order.
     */
    fun customer(order: Order, env: DataFetchingEnvironment): CompletableFuture<Customer> =
            prepareDataLoader(env) { env.requestContext.customerDataLoader }.load(order.customer.entityId)
}