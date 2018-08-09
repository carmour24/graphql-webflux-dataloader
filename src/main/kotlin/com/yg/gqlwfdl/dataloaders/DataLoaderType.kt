package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.RequestContext
import org.dataloader.DataLoaderRegistry

/**
 * Enumeration of all the different types of data loaders available in the system. Each one can be used to create an
 * instance of [EntityDataLoader].
 */
enum class DataLoaderType {
    COMPANY, CUSTOMER, COMPANY_PARTNERSHIP, PRICING_DETAILS, PRODUCT, PRODUCT_ORDER_COUNT;

    /**
     * The key under which this data loader is stored in the [DataLoaderRegistry], stored in the
     * [RequestContext.dataLoaderRegistry] property.
     */
    val registryKey
        get() = toString()
}