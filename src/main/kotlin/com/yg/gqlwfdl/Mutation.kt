package com.yg.gqlwfdl

import com.yg.gqlwfdl.unitofwork.UnitOfWork
import java.util.concurrent.CompletionStage

/**
 * Mutation interface to encapsulate the work to be performed in a mutation, in [action], and the data to be
 * returned, from [result], after the action has completed and the [UnitOfWork] relating to the mutation has been
 * successfully committed.
 * This type is required as the data to be returned might require communicating with multiple sources and so could be
 * long running and therefore we would not wish to perform this while the unit of work's transaction was open.
 * Therefore we need a way to signal that the unit of work should complete so that we can retrieve the result, this
 * provides a way to have this done for us automatically.
 */
interface Mutation<TReturn> {
    /**
     * Action to be performed as this [unitOfWork]. Returns a [CompletionStage] completing with any type, to signal
     * completion.
     */
    fun action(unitOfWork: UnitOfWork) : CompletionStage<*>

    /**
     * Data to be returned after the [UnitOfWork] has completed, this is made available when complete via a
     * [CompletionStage] of [TReturn].
     */
    fun result() : CompletionStage<TReturn>
}