package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.ClientField
import com.yg.gqlwfdl.services.Entity

/**
 * Contains information about a request for [Entity] objects, typically when initiated from a request for such data from
 * the client.
 *
 * @property childFields The child fields being requested on the requested entities.
 * @property creationListener The object to inform when entities are instantiated, e.g. to cache them in a data loader.
 */
class EntityRequestInfo(val childFields: List<ClientField> = listOf(),
                        val creationListener: EntityCreationListener? = null) {

    /**
     * Gets a boolean indicating whether this object's [childFields] contain one which has the passed in [name]. Only
     * looks at the immediate children: not at their children in turn (i.e. this is not a recursive check).
     */
    fun containsField(name: String) = childFields.map { it.name }.contains(name)
}