package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.dataaccess.EntityCreationListener
import com.yg.gqlwfdl.dataloaders.DataLoaderType

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
    private val cachers: Map<Class<out Entity<*>>, EntityCacher<out Any, *>> = listOf(
            EntityCacher(DataLoaderType.CUSTOMER, Customer::class.java),
            EntityCacher(DataLoaderType.COMPANY, Company::class.java),
            EntityCacher(DataLoaderType.COMPANY_PARTNERSHIP, CompanyPartnership::class.java),
            EntityCacher(DataLoaderType.PRICING_DETAILS, PricingDetails::class.java),
            EntityCacher(DataLoaderType.PRODUCT, Product::class.java),
            EntityCacher(DataLoaderType.PRODUCT_ORDER_COUNT, ProductOrderCount::class.java)
    ).map { it.entityClass to it }.toMap()

    override fun onEntityCreated(entity: Entity<*>) {
        cachers[entity.javaClass]?.cache(requestContext, entity)
    }
}

/**
 * An object responsible for caching [TEntity] objects into a data loader of the specified [dataLoaderType].
 *
 * @property dataLoaderType The type of data loader which this object caches entities into.
 * @property entityClass The [Class] of [TEntity].
 */
private class EntityCacher<TId, TEntity : Entity<out TId>>(
        private val dataLoaderType: DataLoaderType,
        val entityClass: Class<TEntity>) {

    /**
     * Caches the passed in entity into the [requestContext]'s data loader.
     */
    fun cache(requestContext: RequestContext, entity: Any) {
        // Can safely cast here as this private function is only called from one single place, where type checking has
        // implicitly already been done.
        @Suppress("UNCHECKED_CAST")
        requestContext.dataLoader<TId, TEntity>(dataLoaderType).prime(entity as TEntity)
    }
}