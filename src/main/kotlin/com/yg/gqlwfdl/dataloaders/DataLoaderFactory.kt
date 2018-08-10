package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.dataaccess.EntityCreationListener
import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import com.yg.gqlwfdl.services.*
import org.dataloader.DataLoaderRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture

/**
 * Class responsible for creating all the [EntityDataLoader]s in the system.
 */
@Component
class DataLoaderFactory(private val customerService: CustomerService,
                        private val companyService: CompanyService,
                        private val companyPartnershipService: CompanyPartnershipService,
                        private val pricingDetailsService: PricingDetailsService,
                        private val productService: ProductService) {

    /**
     * Creates a new instance of every data loader (for all the items in [DataLoaderType]) and registers it with the
     * passed in [DataLoaderRegistry].
     *
     * @param registry The registry with which to register each of the data loaders.
     * @param requestContext The object providing access to the current request context.
     */
    fun createAllAndRegister(registry: DataLoaderRegistry, requestContext: RequestContext) {
        DataLoaderType.values().forEach { dataLoaderType ->
            // Use a "when" to ensure that every type is included: compiler will fail if not every entry in the enum
            // is handled.
            val dataLoader = when (dataLoaderType) {
                DataLoaderType.COMPANY -> createDataLoader<Long, Company>(requestContext) { ids, requestInfo ->
                    companyService.findByIds(ids, requestInfo)
                }

                DataLoaderType.CUSTOMER -> createDataLoader<Long, Customer>(requestContext) { ids, requestInfo ->
                    customerService.findByIds(ids, requestInfo)
                }

                DataLoaderType.COMPANY_PARTNERSHIP -> createDataLoader<Long, CompanyPartnership>(requestContext) { ids, requestInfo ->
                    companyPartnershipService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRICING_DETAILS -> createDataLoader<Long, PricingDetails>(requestContext) { ids, requestInfo ->
                    pricingDetailsService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRODUCT -> createDataLoader<Long, Product>(requestContext) { ids, requestInfo ->
                    productService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRODUCT_ORDER_COUNT -> createDataLoader<Long, ProductOrderCount>(requestContext) { ids, requestInfo ->
                    productService.findWithOrderCount(ids, requestInfo)
                }
            }

            registry.register(dataLoaderType.registryKey, dataLoader)
        }
    }

    /**
     * Creates an [EntityDataLoader] which will use the passed in [batchLoadFunction] to get all the entities in a single
     * operation, when required. Uses the passed in [requestContext] to get at an [EntityCreationListener] so that this
     * can be sed to create an [EntityRequestInfo] to then pass into service methods for fetching entities. Also creates
     * a [ClientFieldStore] to be used by the data loader to know what child fields are requested for the entities, so
     * that the correct database joins can be added to minimise the number of hits to the database.
     */
    private fun <TId, TEntity : Entity<TId>> createDataLoader(
            requestContext: RequestContext,
            batchLoadFunction: (List<TId>, EntityRequestInfo) -> CompletableFuture<List<TEntity>>)
            : EntityDataLoader<TId, TEntity> {

        val childFieldStore = ClientFieldStore()
        return EntityDataLoader(childFieldStore) {
            batchLoadFunction(it,
                    EntityRequestInfo(childFieldStore.fields, requestContext.dataLoaderPrimerEntityCreationListener))
        }
    }
}

// Below are the extension methods on RequestContext to provide easy access to all the data loaders created above.

/**
 * Gets the data loader for caching/loading customers ([Customer] objects).
 */
val RequestContext.customerDataLoader
    get() = this.dataLoader<Long, Customer>(DataLoaderType.CUSTOMER)

/**
 * Gets the data loader for caching/loading companies ([Company] objects).
 */
val RequestContext.companyDataLoader
    get() = this.dataLoader<Long, Company>(DataLoaderType.COMPANY)

/**
 * Gets the data loader for caching/loading company partnerships ([CompanyPartnership] objects).
 */
@Suppress("unused")
val RequestContext.companyPartnershipDataLoader
    get() = this.dataLoader<Long, CompanyPartnership>(DataLoaderType.COMPANY)

/**
 * Gets the data loader for caching/loading companies ([Company] objects).
 */
val RequestContext.pricingDetailsDataLoader
    get() = this.dataLoader<Long, PricingDetails>(DataLoaderType.PRICING_DETAILS)

/**
 * Gets the data loader for caching/loading products ([Product] objects).
 */
@Suppress("unused")
val RequestContext.productDataLoader
    get() = this.dataLoader<Long, Product>(DataLoaderType.PRODUCT)

/**
 * Gets the data loader for caching/loading products ([Product] objects).
 */
val RequestContext.productOrderCountDataLoader
    get() = this.dataLoader<Long, ProductOrderCount>(DataLoaderType.PRODUCT_ORDER_COUNT)