package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.ClientField
import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.services.Entity

/**
 * Contains information about a request for [Entity] objects, typically when initiated from a request for such data from
 * the client.
 *
 * @property context The context within which this request was made, typically used to get access to the
 * [EntityCreationListener] to inform when entities are instantiated, e.g. to cache them in a data loader.
 * @property childFields The child fields being requested on the requested entities. Defaults to an empty list if not
 * specified.
 * @property entityCreationListener The object to be informed when [Entity] objects are created. Defaults to the passed
 * in [context]'s [entityCreationListener][RequestContext.entityCreationListener], if specified.
 */
class EntityRequestInfo(val context: RequestContext?,
                        val childFields: List<ClientField> = listOf(),
                        val entityCreationListener: EntityCreationListener? = context?.entityCreationListener) {

    /**
     * Gets a boolean indicating whether this object's [childFields] contain one which has the passed in [name]. Only
     * looks at the immediate children: not at their children in turn (i.e. this is not a recursive check).
     */
    fun containsChildField(name: String) = childFields.map { it.name }.contains(name)
}