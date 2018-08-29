package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import org.jooq.Field
import kotlin.reflect.KProperty1

/**
 * Maps the properties in an entity to the fields in a database table.
 */
interface EntityPropertyToTableFieldMapper<TEntity, TField> {
    /**
     * Maps persistence store fields, of type [TField], in [fieldList] to [memberProperties] of [TEntity]. Returning
     * an ordered list of entity properties corresponding to the order of [fieldList]
     */
    fun mapEntityPropertiesToTableFields(memberProperties: Collection<KProperty1<TEntity, *>>,
                                         fieldList:
                                         List<TField>): List<KProperty1<TEntity, *>?>
}

/**
 * Default mapper of table fields to entity properties.
 *
 * Matches by performing the following operations:
 * 1. replacing '_' with '' in the table field name
 * 2. lowercase both the table field name and the entity property name
 * 3. perform a compare on the normalised property name and field name
 * 4. if the above compare fails, check if the property name ends in 'id' and if so, remove and compare this new value
 *
 * This implementation doesn't support:
 * * matching property types to field types
 * * fields differing by case only
 * * multiple fields differing only by one having suffix 'id' or '_id', e.g. 'customer' and 'customer_id'
 */
class DefaultEntityPropertyToTableFieldMapper<TEntity : Entity<*>> : EntityPropertyToTableFieldMapper<TEntity,
        Field<*>> {

    /**
     * Map database table fields to entity properties.
     *
     * @return An ordered list of entity properties corresponding to the order of [fieldList]
     */
    override fun mapEntityPropertiesToTableFields(memberProperties:
                                                  Collection<KProperty1<TEntity, *>>,
                                                  fieldList:
                                                  List<Field<*>>): List<KProperty1<TEntity, *>?> {
        // Use a mutable list to remove the found entries so on each iteration of the search for a field we are
        // only searching through properties which have not previously been identified.
        val unidentifiedEntityProperties = memberProperties.toMutableList()

        return fieldList.map { field ->
            // Normalise field name by removing '_' and making lowercase.
            val normalisedFieldName = field.name.replace("_", "").toLowerCase()

            val property = unidentifiedEntityProperties.find {
                // Normalise property name by making lowercase.
                val normalisedPropName = it.name.toLowerCase()

                // Compare filed name and property name. If no match then compare again with "id" removed from the
                // end of the normalised property name. It is fairly common to specify the ID of a foreign key field
                // ending in Id on a property but this may match up with a database field with name without the suffix.
                val found = (normalisedPropName == normalisedFieldName)
                        .or(normalisedPropName.endsWith("id") && normalisedPropName.substring(0,
                                normalisedPropName.length - 2) == normalisedFieldName)
                found
            }

            unidentifiedEntityProperties.remove(property)

            property
        }
    }
}