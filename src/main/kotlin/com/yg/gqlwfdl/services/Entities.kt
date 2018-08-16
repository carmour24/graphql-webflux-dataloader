package com.yg.gqlwfdl.services

import sun.jvm.hotspot.gc.serial.TenuredGeneration
import java.time.OffsetDateTime

/**
 * Interface which all entities (domain model objects) implement.
 *
 * @param TId The data type of the unique [id] of this entity.
 */
interface Entity<TData, TId> {
    /**
     * The unique identifier of this entity.
     */
    val id: TId
}

/**
 * Interface representing an object which wraps an [Entity] and exposes it. Also able to act as an entity itself. Allows
 * the Decorator pattern to be used to add (potentially multiple pieces of) functionality/data to entities.
 */
interface EntityWrapper<TId, TData, TEntity : Entity<TData, TId>> : Entity<TData, TId> {
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

interface CustomerData {
    var firstName: String
    var lastName: String
    var companyId: Long
    var pricingDetailsId: Long
    var outOfOfficeDelegate: Long?
}

class Customer(customerData: CustomerData, override val id: Long) : CustomerData by customerData, Entity<CustomerData,
        Long>

data class Company(
        var name: String,
        var address: String,
        var pricingDetailsId: Long,
        var primaryContact: Long? = null) : Entity

data class CompanyPartnership(
        val companyA: Company,
        val companyB: Company) : Entity

data class VatRate(
        var description: String,
        var value: Double) : Entity

data class DiscountRate(
        var description: String,
        var value: Double) : Entity

data class PaymentMethod(
        var description: String,
        var charge: Double) : Entity

data class PricingDetails(
        var description: String,
        var vatRate: VatRate,
        var discountRate: DiscountRate,
        var preferredPaymentMethod: PaymentMethod) : Entity

data class Product(
        var description: String,
        var price: Double,
        var companyId: Long) : Entity

data class Order(
        var customerId: Long,
        var date: OffsetDateTime,
        var deliveryAddress: String,
        val lines: List<Line>) : Entity

data class Line(val id: Long,
                var product: Product,
                var price: Double)
}

/**
 * A wrapper around an [Entity] which exposes the entity itself, and a count.
 */
class EntityWithCount<TId, TEntity : PersistedEntity<TId>>(override val entity: TEntity,
                                                           val count: Int) : EntityWrapper<TId, PersistedEntity<TId>>