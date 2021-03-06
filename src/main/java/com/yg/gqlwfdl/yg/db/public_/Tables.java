/*
 * This file is generated by jOOQ.
 */
package com.yg.gqlwfdl.yg.db.public_;


import com.yg.gqlwfdl.yg.db.public_.tables.Company;
import com.yg.gqlwfdl.yg.db.public_.tables.CompanyPartnership;
import com.yg.gqlwfdl.yg.db.public_.tables.Customer;
import com.yg.gqlwfdl.yg.db.public_.tables.DiscountRate;
import com.yg.gqlwfdl.yg.db.public_.tables.PaymentMethod;
import com.yg.gqlwfdl.yg.db.public_.tables.PricingDetails;
import com.yg.gqlwfdl.yg.db.public_.tables.VatRate;

import javax.annotation.Generated;


/**
 * Convenience access to all tables in PUBLIC
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.11.2"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Tables {

    /**
     * The table <code>PUBLIC.COMPANY</code>.
     */
    public static final Company COMPANY = com.yg.gqlwfdl.yg.db.public_.tables.Company.COMPANY;

    /**
     * The table <code>PUBLIC.COMPANY_PARTNERSHIP</code>.
     */
    public static final CompanyPartnership COMPANY_PARTNERSHIP = com.yg.gqlwfdl.yg.db.public_.tables.CompanyPartnership.COMPANY_PARTNERSHIP;

    /**
     * The table <code>PUBLIC.CUSTOMER</code>.
     */
    public static final Customer CUSTOMER = com.yg.gqlwfdl.yg.db.public_.tables.Customer.CUSTOMER;

    /**
     * The table <code>PUBLIC.DISCOUNT_RATE</code>.
     */
    public static final DiscountRate DISCOUNT_RATE = com.yg.gqlwfdl.yg.db.public_.tables.DiscountRate.DISCOUNT_RATE;

    /**
     * The table <code>PUBLIC.PAYMENT_METHOD</code>.
     */
    public static final PaymentMethod PAYMENT_METHOD = com.yg.gqlwfdl.yg.db.public_.tables.PaymentMethod.PAYMENT_METHOD;

    /**
     * The table <code>PUBLIC.PRICING_DETAILS</code>.
     */
    public static final PricingDetails PRICING_DETAILS = com.yg.gqlwfdl.yg.db.public_.tables.PricingDetails.PRICING_DETAILS;

    /**
     * The table <code>PUBLIC.VAT_RATE</code>.
     */
    public static final VatRate VAT_RATE = com.yg.gqlwfdl.yg.db.public_.tables.VatRate.VAT_RATE;
}
