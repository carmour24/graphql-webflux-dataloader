package com.yg.gqlwfdl

import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import graphql.language.Field
import graphql.schema.DataFetchingEnvironment

/**
 * Gets a list of all the child fields of the receiver.
 */
val Field.childFields
    get() = this.selectionSet?.selections?.filter { it is Field }?.map { it as Field } ?: listOf()

/**
 * Creates an instance of [ClientField] based on `this` GraphQL [Field], including mapping all its children.
 */
fun Field.toClientField(): ClientField = ClientField(this.name, this.childFields.map { it.toClientField() })

/**
 * Gets an [EntityRequestInfo] from `this` (a [DataFetchingEnvironment]).
 */
fun DataFetchingEnvironment.toEntityRequestInfo() = EntityRequestInfo(
        this.field.childFields.map { it.toClientField() }, this.requestContext.dataLoaderPrimerEntityCreationListener)

/**
 * Gets the [RequestContext] of the current request from `this` (a [DataFetchingEnvironment]). Assumes that the
 * environment has had a [RequestContext] set on it, which it always should, otherwise an exception will be thrown.
 */
val DataFetchingEnvironment.requestContext: RequestContext
    get() = this.getContext<RequestContext>()