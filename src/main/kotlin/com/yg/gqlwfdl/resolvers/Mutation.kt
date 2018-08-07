package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLMutationResolver
import com.yg.gqlwfdl.TestDataCreator
import com.yg.gqlwfdl.dataaccess.DBConfig

@Suppress("unused")
/**
 * Class containing the mutations (e.g. inserts, updates) invoked by GraphQL requests.
 */
class Mutation(private val dbConfig: DBConfig) : GraphQLMutationResolver {

    /**
     * Deletes all existing data and populates the database with a bunch of randomly generated test data.
     */
    fun createTestData(): String {
        TestDataCreator(dbConfig).execute()
        return "Test data successfully created"
    }
}