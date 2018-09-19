package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.ChangeType
import com.opidis.unitofwork.data.Entity
import com.opidis.unitofwork.data.QueryMappingConfiguration
import com.yg.gqlwfdl.dataaccess.CustomerRepository
import com.yg.gqlwfdl.dataaccess.OrderLineRepository
import com.yg.gqlwfdl.dataaccess.OrderRepository
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.Order
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KClass

typealias QueryAction = (PgClientExecutionInfo?) -> CompletionStage<IntArray>

@Component
class QueryMappingConfiguration(
        private val customerRepository: CustomerRepository,
        private val orderRepository: OrderRepository,
        private val orderLineRepository: OrderLineRepository) :
        QueryMappingConfiguration<QueryAction> {
    override fun <T : Entity> queryFor(changeType: ChangeType, entities: List<T>): QueryAction {
        return when (entities.first()) {
            is Customer -> queryForCustomers(changeType, entities.asSequence().map { it as Customer }.toList())

            is Order -> queryForOrders(changeType, entities.asSequence().map { it as Order }.toList())

            is Order.Line -> queryForOrderLines(changeType, entities.asSequence().map { it as Order.Line }.toList())

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
                            // TODO: Check for success before completing with 1
                            futureWithCount.complete(IntArray(insertedCustomerIDs.size) { 1 })
                            futureWithCount
                        }
            }
        }
    }

    private fun queryForOrders(changeType: ChangeType, orderEntities: List<Order>): QueryAction {
        return when (changeType) {
            ChangeType.Delete -> { executionInfo -> orderRepository.delete(orderEntities, executionInfo) }
            ChangeType.Update -> { executionInfo -> orderRepository.update(orderEntities, executionInfo) }
            ChangeType.Insert -> { executionInfo ->
                orderRepository.insert(orderEntities, executionInfo)
                        .thenCompose { insertedOrderIDs ->
                            val futureWithCount = CompletableFuture<IntArray>()
                            // TODO: Check for success before completing with 1
                            futureWithCount.complete(IntArray(insertedOrderIDs.size) { 1 })
                            futureWithCount
                        }
            }
        }
    }

    private fun queryForOrderLines(changeType: ChangeType, orderLineEntities: List<Order.Line>): QueryAction {
        return when (changeType) {
            ChangeType.Delete -> { executionInfo -> orderLineRepository.delete(orderLineEntities, executionInfo) }
            ChangeType.Update -> { executionInfo -> orderLineRepository.update(orderLineEntities, executionInfo) }
            ChangeType.Insert -> { executionInfo ->
                orderLineRepository.insert(orderLineEntities, executionInfo)
                        .thenCompose { insertedOrderLineIDs ->
                            val futureWithCount = CompletableFuture<IntArray>()
                            // TODO: Check for success before completing with 1
                            futureWithCount.complete(IntArray(insertedOrderLineIDs.size) { 1 })
                            futureWithCount
                        }
            }
        }
    }
    class TypeMappingNotFoundException(clazz: KClass<*>)
        : Exception("No query mapping for class of type ${clazz.qualifiedName} could be found.")
}
