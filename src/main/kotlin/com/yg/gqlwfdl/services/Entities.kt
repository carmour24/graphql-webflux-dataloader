package com.yg.gqlwfdl.services

import java.time.OffsetDateTime
import java.util.*

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

typealias CustomerID = UUID
typealias CompanyID = UUID
typealias CompanyPartnershipID = UUID
typealias VatRateID = UUID
typealias DiscountRateID = UUID
typealias PaymentMethodID = UUID
typealias PricingDetailsID = UUID
typealias ProductID = UUID
typealias OrderID = UUID
typealias LineID = UUID

data class Customer(
        override val id: CustomerID,
        var firstName: String,
        var lastName: String,
        var companyId: Long,
        var pricingDetailsId: Long,
        var outOfOfficeDelegate: Long?
) : Entity<CustomerID>


data class Company(
        override val id: CompanyID,
        var name: String,
        var address: String,
        var pricingDetailsId: Long,
        var primaryContact: Long? = null) : Entity<CompanyID>

data class CompanyPartnership(
        override val id: CompanyPartnershipID,
        val companyA: Company,
        val companyB: Company) : Entity<CompanyPartnershipID>

data class VatRate(
        override val id: VatRateID,
        var description: String,
        var value: Double) : Entity<VatRateID>

data class DiscountRate(
        override val id: DiscountRateID,
        var description: String,
        var value: Double
) : Entity<DiscountRateID>

data class PaymentMethod(
        override val id: PaymentMethodID,
        var description: String,
        var charge: Double
) : Entity<PaymentMethodID>

data class PricingDetails(
        override val id: PricingDetailsID,
        var description: String,
        var vatRate: VatRate,
        var discountRate: DiscountRate,
        var preferredPaymentMethod: PaymentMethod
) : Entity<PricingDetailsID>

data class Product(
        override val id: ProductID,
        var description: String,
        var price: Double,
        var companyId: Long
) : Entity<ProductID>

data class Order(
        override val id: OrderID,
        var customer: Customer,
        var date: OffsetDateTime,
        var deliveryAddress: String,
        val lines: List<Line>
) : Entity<OrderID>

data class Line(
        override val id: LineID,
        var product: Product,
        var price: Double
) : Entity<LineID>


/**
 * A wrapper around an [Entity] which exposes the entity itself, and a count.
 */
class EntityWithCount<TId, TEntity : Entity<TId>>(override val entity: TEntity,
                                                  val count: Int) : EntityWrapper<TId, Entity<TId>>