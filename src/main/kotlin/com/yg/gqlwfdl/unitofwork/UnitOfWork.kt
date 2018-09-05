package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.DefaultEntityTrackingUnitOfWork
import com.opidis.unitofwork.data.Entity
import java.util.concurrent.CompletionStage

class UnitOfWork(queryMappingConfiguration: QueryMappingConfiguration, queryCoordinator: QueryCoordinator) :
        DefaultEntityTrackingUnitOfWork<QueryAction>(queryMappingConfiguration, queryCoordinator){
    private val hashMap = HashMap<Entity, Int>()
    fun trackEntityForChanges(entity: Entity) {
        hashMap[entity] = entity.hashCode()
    }

    override fun complete(): CompletionStage<Void> {
        // Run through all entities checking for changes and inserting them into the changed entity list
        hashMap.forEach { entity, hashCode ->
            if (entity.hashCode() != hashCode) {
                trackChange(entity)
            }
        }

        return super.complete()
    }
}