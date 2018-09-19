package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.EntityCreationListener

class CompositeEntityCreationListener(vararg entityCreationListeners: EntityCreationListener) :EntityCreationListener {
    private val entityCreationListeners: Array<EntityCreationListener> = arrayOf(*entityCreationListeners)

    override fun onEntityCreated(entity: Entity<*>) {
        entityCreationListeners.forEach {
            it.onEntityCreated(entity)
        }
    }
}