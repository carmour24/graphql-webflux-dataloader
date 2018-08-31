package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.ChangeType
import com.opidis.unitofwork.data.Entity
import com.opidis.unitofwork.data.QueryMappingConfiguration
import com.yg.gqlwfdl.dataaccess.CustomerRepository
import com.yg.gqlwfdl.services.Customer
import kotlin.reflect.KClass

typealias QueryAction = () -> Unit

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
            ChangeType.Delete -> { -> }
            ChangeType.Update -> { -> customerRepository.update(customerEntities) }
            ChangeType.Insert -> { -> customerRepository.insert(customerEntities) }
        }
    }

    class TypeMappingNotFoundException(clazz: KClass<*>)
        : Exception("No query mapping for class of type ${clazz.qualifiedName} could be found.")
}

