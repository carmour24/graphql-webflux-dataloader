package com.yg.gqlwfdl

/**
 * A field requested by the client, generally corresponding to a GraphQL field.
 */
class ClientField(val name: String, val children: List<ClientField> = listOf())