package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLResolver
import com.yg.gqlwfdl.dataloaders.companyDataLoader
import com.yg.gqlwfdl.dataloaders.productOrderCountDataLoader
import com.yg.gqlwfdl.requestContext
import com.yg.gqlwfdl.services.Company
import com.yg.gqlwfdl.services.Product
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

@Suppress("unused")
/**
 * Resolver for [Product]s. Provides access to properties which the GraphQL schema exposes of these objects, but which
 * don't exist directly on the domain model object (Product in this case), and need to be queried for separately. This
 * is done by delegating the work to the data loaders, so that the N+1 problem is bypassed, and the fetches can be
 * batched in one single call.
 */
class ProductResolver : DataLoadingResolver(), GraphQLResolver<Product> {

    /**
     * Gets a [CompletableFuture] which, when completed, will return the company for the passed in product.
     */
    fun company(product: Product, env: DataFetchingEnvironment): CompletableFuture<Company> =
            prepareDataLoader(env) { env.requestContext.companyDataLoader }.load(product.companyId)

    /**
     * Gets a [CompletableFuture] which, when completed, will return the number of orders for the passed in product.
     */
    fun orderCount(product: Product, env: DataFetchingEnvironment): CompletableFuture<Int> =
            prepareDataLoader(env) { env.requestContext.productOrderCountDataLoader }.load(product.id)
                    .thenApply { it ?: 0 }
}