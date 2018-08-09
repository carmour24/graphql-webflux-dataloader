package com.yg.gqlwfdl.services

import java.time.OffsetDateTime

/**
 * Interface which all entities (domain model objects) implement.
 *
 * @param TId The data type of the unique [id] of this entity.
 */
interface Entity<TId> {
    /**
     * The unique identifier of this entity.
     */
    val id: TId
}

/**
 * Interface representing an object which wraps an [Entity] and exposes it. Also able to act as an entity itself. Allows
 * the Decorator pattern to be used to add (potentially multiple pieces of) functionality/data to entities.
 */
interface EntityWrapper<TId, TEntity : Entity<TId>> : Entity<TId> {
    /**
     * The wrapped entity.
     */
    val entity: TEntity

    /**
     * The ID of this entity: defaults to the ID of the wrapped entity itself.
     */
    override val id: TId
        get() = entity.id
}

data class Customer(override val id: Long,
                    var firstName: String,
                    var lastName: String,
                    var companyId: Long,
                    var pricingDetailsId: Long,
                    var outOfOfficeDelegate: Long? = null) : Entity<Long>

data class Company(override val id: Long,
                   var name: String,
                   var address: String,
                   var pricingDetailsId: Long,
                   var primaryContact: Long? = null) : Entity<Long>

data class CompanyPartnership(override val id: Long,
                              val companyA: Company,
                              val companyB: Company) : Entity<Long>

data class VatRate(override val id: Long,
                   var description: String,
                   var value: Double) : Entity<Long>

data class DiscountRate(override val id: Long,
                        var description: String,
                        var value: Double) : Entity<Long>

data class PaymentMethod(override val id: Long,
                         var description: String,
                         var charge: Double) : Entity<Long>

data class PricingDetails(override val id: Long,
                          var description: String,
                          var vatRate: VatRate,
                          var discountRate: DiscountRate,
                          var preferredPaymentMethod: PaymentMethod) : Entity<Long>

data class Product(override val id: Long,
                   var description: String,
                   var price: Double,
                   var companyId: Long) : Entity<Long>

data class Order(override val id: Long,
                 var customerId: Long,
                 var date: OffsetDateTime,
                 var deliveryAddress: String) : Entity<Long>

/**
 * A wrapper around an [Entity] which exposes the entity itself, and a count.
 */
abstract class EntityWithCount<TId, TEntity : Entity<TId>>(override val entity: TEntity,
                                                           val count: Int) : EntityWrapper<TId, Entity<TId>>

/**
 * Represent a [Product] along with the number of orders for it.
 */
// TODO: make this a data class, or get rid of superclass? Ideally this class wouldn't even exist, but because of type
// erasure we need a concrete class rather than multiple instances of EntityWithCount for various bits of code that rely
// on being able to identify a class, e.g. DataLoaderPrimerEntityCreationListener
class ProductOrderCount(entity: Product, count: Int) : EntityWithCount<Long, Product>(entity, count)