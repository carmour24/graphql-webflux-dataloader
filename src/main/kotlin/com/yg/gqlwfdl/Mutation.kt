package com.yg.gqlwfdl

import com.yg.gqlwfdl.unitofwork.UnitOfWork
import java.util.concurrent.CompletionStage

interface Mutation<TReturn> {
    fun action(unitOfWork: UnitOfWork) : CompletionStage<*>

    fun getResult() : CompletionStage<TReturn>
}