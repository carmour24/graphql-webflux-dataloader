package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.dataaccess.EntityCreationListener
import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import com.yg.gqlwfdl.services.*
import org.dataloader.DataLoaderRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture

/**
 * Class responsible for creating all the [ContextAwareDataLoader]s in the system.
 */
@Component
class DataLoaderFactory(private val customerService: CustomerService,
                        private val companyService: CompanyService,
                        private val companyPartnershipService: CompanyPartnershipService,
                        private val pricingDetailsService: PricingDetailsService,
                        private val productService: ProductService,
                        private val orderService: OrderService) {

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
                DataLoaderType.COMPANY -> createSimpleDataLoader<Long, Company>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    companyService.findByIds(ids, requestInfo)
                }

                DataLoaderType.CUSTOMER -> createSimpleDataLoader<Long, Customer>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    customerService.findByIds(ids, requestInfo)
                }

                DataLoaderType.COMPANY_PARTNERSHIP -> createSimpleDataLoader<Long, CompanyPartnership>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    companyPartnershipService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRICING_DETAILS -> createSimpleDataLoader<Long, PricingDetails>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    pricingDetailsService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRODUCT -> createSimpleDataLoader<Long, Product>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    productService.findByIds(ids, requestInfo)
                }

                DataLoaderType.PRODUCT_ORDER_COUNT -> createBaseDataLoader<Long, Int>(
                        requestContext) { ids, requestInfo ->
                    // Get the products with their counts, then sync those values with the passed in keys (ids), then
                    // extract the count of each item (some of might be null if there are no orders for that product).
                    productService.findWithOrderCount(ids, requestInfo)
                            .thenApply { productsWithCounts ->
                                productsWithCounts.syncWithKeys(ids) { it.id }
                                        .map { it?.count }
                            }
                }

                DataLoaderType.ORDER -> createSimpleDataLoader<Long, Order>(
                        requestContext, { it.id!! }) { ids, requestInfo ->
                    orderService.findByIds(ids, requestInfo)
                }

                DataLoaderType.CUSTOMER_ORDER -> createGroupingDataLoader<Long, Order>(
                        requestContext, { it.customer.entityId!! }) { ids, requestInfo ->
                    orderService.findByCustomerIds(ids, requestInfo)
                }
            }

            registry.register(dataLoaderType.registryKey, dataLoader)
        }
    }

    /**
     * Creates an [ContextAwareDataLoader] which will use the passed in [batchLoadFunction] to get all values in a single
     * operation, when required. Uses the passed in [requestContext] to get at an [EntityCreationListener] so that this
     * can be used to create an [EntityRequestInfo] to then pass into service methods for fetching entities. Also creates
     * a [ClientFieldStore] to be used by the data loader to know what child fields are requested for the entities, so
     * that the correct database joins can be added to minimise the number of hits to the database.
     *
     * This method is used instead of [createSimpleDataLoader] and [createGroupingDataLoader] when the values being
     * cached don't actually have IDs directly (the key they're stored against in the data loader acts as that). When
     * this method is used, the caller should ensure that the values being returned are in sync with the keys supplied.
     */
    private fun <K, V> createBaseDataLoader(
            requestContext: RequestContext,
            batchLoadFunction: (List<K>, EntityRequestInfo) -> CompletableFuture<List<V?>>)
            : ContextAwareDataLoader<K, V> {

        val childFieldStore = ClientFieldStore()
        return ContextAwareDataLoader(childFieldStore) {
            batchLoadFunction(it, EntityRequestInfo(requestContext, childFieldStore.fields))
        }
    }

    /**
     * Creates an [SimpleDataLoader] which will use the passed in [batchLoadFunction] to get all values in a single
     * operation, when required. Uses the passed in [requestContext] to get at an [EntityCreationListener] so that this
     * can be used to create an [EntityRequestInfo] to then pass into service methods for fetching entities. Also creates
     * a [ClientFieldStore] to be used by the data loader to know what child fields are requested for the entities, so
     * that the correct database joins can be added to minimise the number of hits to the database.
     */
    private fun <K, V> createSimpleDataLoader(
            requestContext: RequestContext,
            keySelector: (V) -> K,
            batchLoadFunction: (List<K>, EntityRequestInfo) -> CompletableFuture<List<V>>)
            : SimpleDataLoader<K, V> {

        val childFieldStore = ClientFieldStore()
        return SimpleDataLoader(childFieldStore, keySelector) {
            batchLoadFunction(it, EntityRequestInfo(requestContext, childFieldStore.fields))
        }
    }

    /**
     * Creates an [GroupingDataLoader] which will use the passed in [batchLoadFunction] to get all values in a single
     * operation, when required. Uses the passed in [requestContext] to get at an [EntityCreationListener] so that this
     * can be used to create an [EntityRequestInfo] to then pass into service methods for fetching entities. Also creates
     * a [ClientFieldStore] to be used by the data loader to know what child fields are requested for the entities, so
     * that the correct database joins can be added to minimise the number of hits to the database.
     */
    private fun <K, V> createGroupingDataLoader(
            requestContext: RequestContext,
            keySelector: (V) -> K,
            batchLoadFunction: (List<K>, EntityRequestInfo) -> CompletableFuture<List<V>>)
            : GroupingDataLoader<K, V> {

        val childFieldStore = ClientFieldStore()
        return GroupingDataLoader(childFieldStore, keySelector) {
            batchLoadFunction(it, EntityRequestInfo(requestContext, childFieldStore.fields))
        }
    }
}

// Below are the extension methods on RequestContext to provide easy access to all the data loaders created above.

/**
 * Gets the data loader for caching/loading customers ([Customer] objects).
 */
val RequestContext.customerDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, Customer>(DataLoaderType.CUSTOMER.registryKey) as SimpleDataLoader

/**
 * Gets the data loader for caching/loading companies ([Company] objects).
 */
val RequestContext.companyDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, Company>(DataLoaderType.COMPANY.registryKey) as SimpleDataLoader

/**
 * Gets the data loader for caching/loading company partnerships ([CompanyPartnership] objects).
 */
val RequestContext.companyPartnershipDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, CompanyPartnership>(DataLoaderType.COMPANY_PARTNERSHIP.registryKey) as SimpleDataLoader

/**
 * Gets the data loader for caching/loading companies ([Company] objects).
 */
val RequestContext.pricingDetailsDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, PricingDetails>(DataLoaderType.PRICING_DETAILS.registryKey) as SimpleDataLoader

/**
 * Gets the data loader for caching/loading products ([Product] objects).
 */
val RequestContext.productDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, Product>(DataLoaderType.PRODUCT.registryKey) as SimpleDataLoader

/**
 * Gets the data loader for caching/loading the order count for [Product] objects. The ID/key in the data loader is the
 * [Product.id], and the value is count of orders for that product.
 */
val RequestContext.productOrderCountDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, Int>(DataLoaderType.PRODUCT_ORDER_COUNT.registryKey) as ContextAwareDataLoader

/**
 * Gets the data loader for caching/loading orders ([Order] objects).
 */
val RequestContext.orderDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, Order>(DataLoaderType.ORDER.registryKey) as SimpleDataLoader


/**
 * Gets the data loader for caching/loading [Order]s which belong to specific [Customer]s
 */
val RequestContext.customerOrderDataLoader
    get() = this.dataLoaderRegistry.getDataLoader<Long, List<Order>>(DataLoaderType.CUSTOMER_ORDER.registryKey) as GroupingDataLoader