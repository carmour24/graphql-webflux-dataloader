package com.yg.gqlwfdl.services

import java.time.OffsetDateTime


/**
 * Interface which all entities (domain model objects) implement.
 *
 * @param TId The data type of the unique [id] of this entity.
 */
interface Entity<TId> : com.opidis.unitofwork.data.Entity {
    /**
     * The unique identifier of this entity.
     */
    var id: TId?
}

/**
 * Interface representing an object which wraps an [Entity] and exposes it. Also able to act as an entity itself. Allows
 * the Decorator pattern to be used to add (potentially multiple pieces of) functionality/data to entities.
 */
abstract class EntityWrapper<TId, TEntity : Entity<TId>>(val entity: TEntity) : Entity<TId> by entity

typealias CustomerID = Long
typealias CompanyID = Long
typealias CompanyPartnershipID = Long
typealias VatRateID = Long
typealias DiscountRateID = Long
typealias PaymentMethodID = Long
typealias PricingDetailsID = Long
typealias ProductID = Long
typealias OrderID = Long
typealias LineID = Long

data class Customer(
        override var id: CustomerID?,
        var firstName: String,
        var lastName: String,
        var companyId: Long,
        var pricingDetailsId: PricingDetailsID,
        var outOfOfficeDelegate: CustomerID? = null
) : Entity<CustomerID>


data class Company(
        override var id: CompanyID?,
        var name: String,
        var address: String,
        var pricingDetailsId: PricingDetailsID,
        var primaryContact: CustomerID? = null) : Entity<CompanyID>

data class CompanyPartnership(
        override var id: CompanyPartnershipID?,
        val companyA: Company,
        val companyB: Company) : Entity<CompanyPartnershipID>

data class VatRate(
        override var id: VatRateID?,
        var description: String,
        var value: Double) : Entity<VatRateID>

data class DiscountRate(
        override var id: DiscountRateID?,
        var description: String,
        var value: Double
) : Entity<DiscountRateID>

data class PaymentMethod(
        override var id: PaymentMethodID?,
        var description: String,
        var charge: Double
) : Entity<PaymentMethodID>

data class PricingDetails(
        override var id: PricingDetailsID?,
        var description: String,
        var vatRate: VatRate,
        var discountRate: DiscountRate,
        var preferredPaymentMethod: PaymentMethod
) : Entity<PricingDetailsID>

data class Product(
        override var id: ProductID?,
        var description: String,
        var price: Double,
        var companyId: CompanyID
) : Entity<ProductID>

data class Order(
        override var id: OrderID?,
        var customer: EntityOrId<Customer, CustomerID>,
        var date: OffsetDateTime,
        var deliveryAddress: String,
        val lines: List<Line>
) : Entity<OrderID> {

    data class Line(
            override var id: LineID?,
            var product: EntityOrId<Product, ProductID>,
            var price: Double,
            var order: EntityOrId<Order, OrderID>
    ) : Entity<LineID>
}


/**
 * A wrapper around an [Entity] which exposes the entity itself, and a count.
 */
class EntityWithCount<TId, TEntity : Entity<TId>>(entity: TEntity, val count: Int)
    : EntityWrapper<TId, TEntity> (entity)
