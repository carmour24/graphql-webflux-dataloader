package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.dataaccess.CompanyPartnershipRecords
import com.yg.gqlwfdl.dataaccess.PricingDetailsRecords
import com.yg.gqlwfdl.dataaccess.db.tables.records.*
import com.yg.gqlwfdl.dataaccess.toEntity
import com.yg.gqlwfdl.services.Entity
import org.springframework.stereotype.Component

/**
 * Object responsible for providing all the [JoinedRecordToEntityConverter]s in the system.
 */
interface JoinedRecordToEntityConverterProvider {
    /**
     * Gets all the [JoinedRecordToEntityConverter]s in the system.
     */
    val converters: List<JoinedRecordToEntityConverter<out Entity<out Any>>>
}

/**
 * Default implementation of [JoinedRecordToEntityConverterProvider], returning all the converters in the example
 * database structure.
 */
@Component
class DefaultRecordToEntityConverterProvider : JoinedRecordToEntityConverterProvider {

    override val converters: List<JoinedRecordToEntityConverter<out Entity<out Any>>> =
            listOf(
                    SingleTypeJoinedRecordToEntityConverter(CustomerRecord::class.java) { it.toEntity() },
                    SingleTypeJoinedRecordToEntityConverter(CompanyRecord::class.java) { it.toEntity() },
                    MultiTypeJoinedRecordToEntityConverter2(CompanyPartnershipRecord::class.java,
                            COMPANY_PARTNERSHIP_COMPANY_A.name,
                            COMPANY_PARTNERSHIP_COMPANY_B.name,
                            CompanyRecord::class.java,
                            CompanyRecord::class.java
                    ) { companyPartnershipRecord, companyARecord, companyBRecord ->
                        CompanyPartnershipRecords(companyPartnershipRecord, companyARecord, companyBRecord).toEntity()
                    },
                    MultiTypeJoinedRecordToEntityConverter3(PricingDetailsRecord::class.java,
                            PRICING_DETAILS_VAT_RATE.name,
                            PRICING_DETAILS_DISCOUNT_RATE.name,
                            PRICING_DETAILS_PREFERRED_PAYMENT_METHOD.name,
                            VatRateRecord::class.java,
                            DiscountRateRecord::class.java,
                            PaymentMethodRecord::class.java
                    ) { pricingDetailsRecord, vatRateRecord, discountRateRecord, paymentMethodRecord ->
                        PricingDetailsRecords(pricingDetailsRecord, vatRateRecord, discountRateRecord, paymentMethodRecord).toEntity()
                    },
                    SingleTypeJoinedRecordToEntityConverter(ProductRecord::class.java) { it.toEntity() }
            )
}