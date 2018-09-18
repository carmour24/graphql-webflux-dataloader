package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.DefaultEntityTrackingUnitOfWork
import com.opidis.unitofwork.data.Entity
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import com.yg.gqlwfdl.getLogger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.logging.Level

class UnitOfWork(queryMappingConfiguration: QueryMappingConfiguration, queryCoordinator: QueryCoordinator) :
        DefaultEntityTrackingUnitOfWork<QueryAction, PgClientExecutionInfo>(queryMappingConfiguration, queryCoordinator) {
    private val hashMap = HashMap<Entity, Int>()
    private var future = CompletableFuture<Void>()

    val completionStage: CompletionStage<Void>
        get() = future

    fun trackEntityForChanges(entity: Entity) {
        hashMap[entity] = entity.hashCode()
    }

    override fun complete(): CompletionStage<Void> {
        // Run through all entities checking for changes and inserting them into the changed entity list
        hashMap.mapNotNull { (entity, hashCode) ->
            if (entity.hashCode() != hashCode) {
                getLogger()?.log(Level.FINER, "Tracked change for entity")
                this.trackChange(entity)
            } else {
                null
            }
        }

        val completion = super.complete()

        completion.whenComplete { t, u ->
            future.complete(null)
//            future = CompletableFuture()
        }

        return completion
    }
}