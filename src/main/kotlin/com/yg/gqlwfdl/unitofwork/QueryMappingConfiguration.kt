package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.ChangeType
import com.opidis.unitofwork.data.Entity
import com.opidis.unitofwork.data.ExecutionInfo
import com.opidis.unitofwork.data.QueryMappingConfiguration
import com.yg.gqlwfdl.dataaccess.CustomerRepository
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import com.yg.gqlwfdl.services.Customer
import io.reactiverse.pgclient.PgClient
import kotlin.reflect.KClass

typealias QueryAction = (PgClientExecutionInfo?) -> Unit

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
            ChangeType.Delete -> { executionInfo -> }
            ChangeType.Update -> { executionInfo -> customerRepository.update(customerEntities, executionInfo) }
            ChangeType.Insert -> { executionInfo -> customerRepository.insert(customerEntities, executionInfo) }
        }
    }

    class TypeMappingNotFoundException(clazz: KClass<*>)
        : Exception("No query mapping for class of type ${clazz.qualifiedName} could be found.")
}
