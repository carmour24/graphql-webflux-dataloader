package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.dataaccess.db.Tables.*

/**
 * Join from the [PRICING_DETAILS] table to the [VAT_RATE] table.
 */
val PRICING_DETAILS_VAT_RATE = JoinDefinition("vatRate", PRICING_DETAILS.VAT_RATE, VAT_RATE.ID)

/**
 * Join from the [PRICING_DETAILS] table to the [DISCOUNT_RATE] table.
 */
val PRICING_DETAILS_DISCOUNT_RATE = JoinDefinition("discountRate", PRICING_DETAILS.DISCOUNT_RATE, DISCOUNT_RATE.ID)

/**
 * Join from the [PRICING_DETAILS] table to the [PAYMENT_METHOD] table.
 */
val PRICING_DETAILS_PREFERRED_PAYMENT_METHOD = JoinDefinition(
        "preferredPaymentMethod", PRICING_DETAILS.PREFERRED_PAYMENT_METHOD, PAYMENT_METHOD.ID)

/**
 * Join from the [COMPANY_PARTNERSHIP] table to the [COMPANY] table, for the "company A" value.
 */
val COMPANY_PARTNERSHIP_COMPANY_A = JoinDefinition("partnershipCompanyA", COMPANY_PARTNERSHIP.COMPANY_A, COMPANY.ID)

/**
 * Join from the [COMPANY_PARTNERSHIP] table to the [COMPANY] table, for the "company B" value.
 */
val COMPANY_PARTNERSHIP_COMPANY_B = JoinDefinition("partnershipCompanyB", COMPANY_PARTNERSHIP.COMPANY_B, COMPANY.ID)

/**
 * Join from the [CUSTOMER] table to the [COMPANY] table.
 */
val CUSTOMER_COMPANY = JoinDefinition("company", CUSTOMER.COMPANY_ID, COMPANY.ID)

/**
 * Join from the [CUSTOMER] table to the [CUSTOMER] table, for the out-of-office delegate value.
 */
val CUSTOMER_OUT_OF_OFFICE_DELEGATE = JoinDefinition("outOfOfficeDelegate", CUSTOMER.OUT_OF_OFFICE_DELEGATE, CUSTOMER.ID)

/**
 * Join from the [CUSTOMER] table to the [PRICING_DETAILS] table.
 */
val CUSTOMER_PRICING_DETAILS = JoinDefinition("pricingDetails", CUSTOMER.PRICING_DETAILS, PRICING_DETAILS.ID)

/**
 * Join from the [COMPANY] table to the [CUSTOMER] table, for the primary contact.
 */
val COMPANY_PRIMARY_CONTACT = JoinDefinition("primaryContact", COMPANY.PRIMARY_CONTACT, CUSTOMER.ID)

/**
 * Join from the [COMPANY] table to the [PRICING_DETAILS] table.
 */
val COMPANY_PRICING_DETAILS = JoinDefinition("pricingDetails", COMPANY.PRICING_DETAILS, PRICING_DETAILS.ID)

/**
 * Join from the [PRODUCT] table to the [COMPANY] table.
 */
val PRODUCT_COMPANY = JoinDefinition("company", PRODUCT.COMPANY, COMPANY.ID)