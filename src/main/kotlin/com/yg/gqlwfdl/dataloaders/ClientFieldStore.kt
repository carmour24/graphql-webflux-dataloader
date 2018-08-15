package com.yg.gqlwfdl.dataloaders

import com.yg.gqlwfdl.ClientField

/**
 * Manages a list of [ClientField]s, providing functionality for "merging" them. Used by [ContextAwareDataLoader]s to
 * keep a list of child fields of the entities being requested. For example, say a request is for a set of users, and
 * each user has a "company" property, which is fetched using this data loader: in this case, this object's list of
 * fields would include that "company" field. The child fields of that company field can then be interrogated to see what
 * company-related information is requested, in case more joins need to be included to fetch this data.
 *
 * Note that a single data loader can be used to provide values for fields requested as part of more than one parent
 * field. For example, a client could make a request tree such as this:
 * ```
 * A
 *   X
 *     Y
 * B
 *   X
 *     Z
 * ```
 * In this case, the X data loader would be called once to get the X values under A and B. When the database is queried,
 * it should join tables Y and Z, to avoid unnecessary round-trips to the database. Therefore the list of fields here
 * should conceptually be a "union" of all the children of the different requested parents. This is what "merging" refers
 * to in this context, i.e. field X would now have two children Y and Z.
 */
class ClientFieldStore {

    /**
     * Backing field for [fields], keyed on the field's name.
     */
    private val _fields = mutableMapOf<String, ClientField>()

    /**
     * The fields managed by this store, typically the child fields of the entity being returned, which were requested
     * by the client as part of a GraphQL request.
     */
    val fields: List<ClientField>
        get() = _fields.values.toList()

    /**
     * Adds the passed in [ClientField]s to the list of those expose in [fields]. Any fields which already exist are
     * "merged" with the existing fields.
     */
    fun addFields(fields: List<ClientField>) {
        fields.forEach { newField ->
            // If the field doesn't exist already, just put this new field in; otherwise merge the existing and new
            // fields into a new field, and put that in. See docs on fields for an explanation.
            _fields[newField.name] =
                    _fields[newField.name]?.let { mergeFields(it, newField) } ?: newField
        }
    }

    /**
     * Merges the two passed in fields, returning a new [ClientField] which has the combined children of the two.
     */
    private fun mergeFields(existingField: ClientField, newField: ClientField): ClientField {
        val newFieldsChildren = newField.children.map { Pair(it.name, it) }.toMap().toMutableMap()

        // Add all the children of the existing field, merging any which the new field has as a child too.
        // And while doing this, remove from newFieldsChildren any that are processed, so that at the end,
        // newFieldsChildren will have all the items we still need to process.
        return ClientField(existingField.name,
                existingField.children.map { existingFieldsChild ->
                    newFieldsChildren[existingField.name]?.let { newFieldsChild ->
                        // Field exists in both new and existing - merge it, and remove from the new fields' children
                        // list
                        newFieldsChildren.remove(newFieldsChild.name)
                        mergeFields(existingFieldsChild, newFieldsChild)
                    } ?: existingFieldsChild
                }.plus(newFieldsChildren.values))
    }
}