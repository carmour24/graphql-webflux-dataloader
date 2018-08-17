package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity

/**
 * Listener for the creation of [Entity] objects. Is informed whenever an entity is constructed so that it can react in
 * some way, typically by caching this entity into a data loader.
 */
interface EntityCreationListener {
    /**
     * Called whenever an entity is created. Typically implementors will do something with the received [entity], such
     * as cache it in a data loader.
     */
    fun onEntityCreated(entity: Entity<*, *>)
}