package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.ChangeType
import com.opidis.unitofwork.data.Entity
import com.opidis.unitofwork.data.ExecutionInfo
import com.opidis.unitofwork.data.QueryMappingConfiguration
import com.yg.gqlwfdl.dataaccess.CustomerRepository
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import com.yg.gqlwfdl.services.Customer
import io.reactiverse.pgclient.PgClient
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KClass

typealias QueryAction = (PgClientExecutionInfo?) -> CompletionStage<IntArray>

@Component
class QueryMappingConfiguration(private val customerRepository: CustomerRepository) :
        QueryMappingConfiguration<QueryAction> {
    override fun <T : Entity> queryFor(changeType: ChangeType, entities: List<T>): QueryAction {
        when (entities.first()) {
            is Customer -> {
                return queryForCustomers(changeType, entities.map { it as Customer }.toList())
            }
            else -> throw TypeMappingNotFoundException(entities.first().javaClass.kotlin)
        }
    }

    private fun queryForCustomers(changeType: ChangeType, customerEntities: List<Customer>): QueryAction {
        return when (changeType) {
            ChangeType.Delete -> { executionInfo -> customerRepository.delete(customerEntities, executionInfo) }
            ChangeType.Update -> { executionInfo -> customerRepository.update(customerEntities, executionInfo) }
            ChangeType.Insert -> { executionInfo ->
                customerRepository.insert(customerEntities, executionInfo)
                        .thenCompose { insertedCustomerIDs ->
                            val futureWithCount = CompletableFuture<IntArray>()
                            futureWithCount.complete(IntArray(insertedCustomerIDs.size, { 1 }))
                            futureWithCount
                        }
            }
        }
    }

    class TypeMappingNotFoundException(clazz: KClass<*>)
        : Exception("No query mapping for class of type ${clazz.qualifiedName} could be found.")
}
