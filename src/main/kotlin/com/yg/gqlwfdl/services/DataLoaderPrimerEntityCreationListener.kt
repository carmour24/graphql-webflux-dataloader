package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.dataaccess.EntityCreationListener
import com.yg.gqlwfdl.dataloaders.*

/**
 * A listener which hears when entities are created (typically after being queried for, from the database). Caches the
 * entities into their relevant data loaders to make them available to subsequent parts of the same GraphQL request.
 */
class DataLoaderPrimerEntityCreationListener(private val requestContext: RequestContext) : EntityCreationListener {

    /**
     * A map of all the known entity cachers. Keyed on the class of the [Entity] being cached, where the value is the
     * [EntityCacher] itself. When requesting an object from this map, if a non-null value is returned for a given class,
     * then the returned value can be safely cast to that class.
     */
    private val cachers: Map<Class<out Entity<*>>, EntityCacher<out Any, *>> = DataLoaderType.values().mapNotNull { type ->
        // Use a "when" to ensure that whenever a new data loader type is added we won't forget to add it here:
        // the compiler will fail if not every entry in the enum is handled.
        when (type) {
            DataLoaderType.COMPANY -> Pair(Company::class.java, EntityCacher(requestContext.companyDataLoader))
            DataLoaderType.CUSTOMER -> Pair(Customer::class.java, EntityCacher(requestContext.customerDataLoader))
            DataLoaderType.COMPANY_PARTNERSHIP -> Pair(CompanyPartnership::class.java, EntityCacher(requestContext.companyPartnershipDataLoader))
            DataLoaderType.PRICING_DETAILS -> Pair(PricingDetails::class.java, EntityCacher(requestContext.pricingDetailsDataLoader))
            DataLoaderType.PRODUCT -> Pair(Product::class.java, EntityCacher(requestContext.productDataLoader))
            DataLoaderType.PRODUCT_ORDER_COUNT -> null
            DataLoaderType.ORDER -> Pair(Order::class.java, EntityCacher(requestContext.orderDataLoader))
            DataLoaderType.CUSTOMER_ORDER -> null
        }
    }.toMap()

    override fun onEntityCreated(entity: Entity<*>) {
        cachers[entity.javaClass]?.cache(entity)
    }
}

/**
 * An object responsible for caching [TEntity] objects into the passed in [dataLoader].
 */
private class EntityCacher<TId, TEntity : Entity<out TId>>(private val dataLoader: SimpleDataLoader<TId, TEntity>) {

    /**
     * Caches the passed in entity into this object's [dataLoader].
     */
    fun cache(entity: Entity<*>) {
        // Can safely cast here as this private function is only called from one single place, where type checking has
        // implicitly already been done.
        @Suppress("UNCHECKED_CAST")
        with(entity as TEntity) { dataLoader.prime(this.id, this) }
    }
}