package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import org.jooq.Field
import kotlin.reflect.KProperty1

/**
 * Maps the properties in an entity to the fields in a database table.
 */
interface EntityPropertyToTableFieldMapper<TEntity, TField> {
    fun mapEntityPropertiesToTableFields(memberProperties: Collection<KProperty1<TEntity, *>>,
                                         fieldList:
                                         List<TField>): List<KProperty1<TEntity, *>?>
}

/**
 * Default mapper of table fields to entity properties.
 *
 * Matches by performing the following operations:
 * * replacing '_' with '' in the table field name
 * * lowercasing both the table field name and the entity property name
 * * perform a compare on the normalised property name and field name
 * * if the above compare fails, check if the proeprty name ends in 'id' and if so, remove and compare this new value
 *
 * Currently doesn't handle ensuring types match up.
 * Will cause issues if there are two fields differing by ending 'id'.
 */
class DefaultEntityPropertyToTableFieldMapper<TEntity : Entity<*>> : EntityPropertyToTableFieldMapper<TEntity,
        Field<*>> {

    /** Map database table fields to entity properties. Using a mutable list and removing the found entries so on each
     * iteration of searching for a field we are only searching through properties which have not previously been identified.
     * TODO: Maybe something like associateby would be more performant/readable.
     */
    override fun mapEntityPropertiesToTableFields(memberProperties:
                                                  Collection<KProperty1<TEntity, *>>,
                                                  fieldList:
                                                  List<Field<*>>): List<KProperty1<TEntity, *>?> {
        val unidentifiedEntityProperties = memberProperties.toMutableList()
        return fieldList.map { field ->
            val normalizedFieldName = field.name.replace("_", "").toLowerCase()
            val property = unidentifiedEntityProperties.find {
                // TODO: Properly map properties to fields somewhere, this mapping of ignoring case and removing
// _ from field names is OK in the interim but needs a reasonable solution.
                val normalizedPropName = it.name.toLowerCase()
                val found = (normalizedPropName == normalizedFieldName)
                        .or(normalizedPropName.endsWith("id") && normalizedPropName.substring(0,
                                normalizedPropName.length - 2) == normalizedFieldName)
                found
            }

            unidentifiedEntityProperties.remove(property)

            property
        }
    }
}