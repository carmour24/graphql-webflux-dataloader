package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLQueryResolver
import com.yg.gqlwfdl.services.*
import com.yg.gqlwfdl.toEntityRequestInfo
import com.yg.gqlwfdl.withLogging
import graphql.schema.DataFetchingEnvironment
import java.util.concurrent.CompletableFuture

@Suppress("unused")
/**
 * The resolver for GraphQL queries. Has methods corresponding to the properties on the Query type in the GraphQL
 * schema.
 */
class Query(private val customerService: CustomerService,
            private val companyService: CompanyService,
            private val companyPartnershipService: CompanyPartnershipService,
            private val productService: ProductService)
    : GraphQLQueryResolver {

    /**
     * Gets all customers in the system.
     */
    fun customers(env: DataFetchingEnvironment): CompletableFuture<List<Customer>> =
            withLogging("getting all customers") { customerService.findAll(env.toEntityRequestInfo()) }

    /**
     * Gets all customers with the passed in IDs.
     */
    fun customersByIds(ids: List<Long>, env: DataFetchingEnvironment): CompletableFuture<List<Customer>> =
            withLogging("getting customers with IDs $ids") { customerService.findByIds(ids, env.toEntityRequestInfo()) }

    /**
     * Gets all companies in the system.
     */
    fun companies(env: DataFetchingEnvironment) =
            withLogging("getting all companies") { companyService.findAll(env.toEntityRequestInfo()) }

    /**
     * Gets all company partnerships in the system.
     */
    fun companyPartnerships(env: DataFetchingEnvironment) =
            withLogging("getting all company partnerships") { companyPartnershipService.findAll(env.toEntityRequestInfo()) }

    /**
     * Gets all products in the system.
     */
    fun products(env: DataFetchingEnvironment) =
            withLogging("getting all products") { productService.findAll(env.toEntityRequestInfo()) }

    /**
     * Gets the [count] top-selling products in the system.
     */
    fun topSellingProducts(count: Int, env: DataFetchingEnvironment): CompletableFuture<List<Product>> {
        // Get the top-selling products - this will return a list of ProductOrderCount (which will have primed the
        // 'env.requestContext.productOrderCountDataLoader' (i.e. stored the order count for each product). Convert
        // the returned value back to simple Product objects. The GraphQL libraries will then the ProductResolver
        // for the order count for each product, and it will in turn ask that data loader, which will be able to use
        // the pre-cached values rather than querying again.
        return withLogging("getting $count top-selling products") {
            productService.findTopSelling(count, env.toEntityRequestInfo())
                    .thenApply { results -> results.map { it.entity } }
        }
    }
}
