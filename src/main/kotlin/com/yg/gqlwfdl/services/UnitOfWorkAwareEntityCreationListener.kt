package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.RequestContext
import com.yg.gqlwfdl.dataaccess.EntityCreationListener

class UnitOfWorkAwareEntityCreationListener(private val requestContext: RequestContext) : EntityCreationListener{
    override fun onEntityCreated(entity: Entity<*>) {
        requestContext.unitOfWork.trackEntityForChanges(entity)
    }
}