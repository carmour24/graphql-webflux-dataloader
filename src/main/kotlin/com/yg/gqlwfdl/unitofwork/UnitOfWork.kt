package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.DefaultEntityTrackingUnitOfWork
import com.opidis.unitofwork.data.Entity
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import java.util.concurrent.CompletionStage

class UnitOfWork(queryMappingConfiguration: QueryMappingConfiguration, queryCoordinator: QueryCoordinator) :
        DefaultEntityTrackingUnitOfWork<QueryAction, PgClientExecutionInfo>(queryMappingConfiguration, queryCoordinator){
    private val hashMap = HashMap<Entity, Int>()
    fun trackEntityForChanges(entity: Entity) {
        hashMap[entity] = entity.hashCode()
    }

    override fun complete(): CompletionStage<Void> {
        // Run through all entities checking for changes and inserting them into the changed entity list
        hashMap.mapNotNull { (entity, hashCode) ->
            if (entity.hashCode() != hashCode) {
                this.trackChange(entity)
            } else {
                null
            }
        }

        return super.complete()
    }
}