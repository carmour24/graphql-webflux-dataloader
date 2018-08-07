package com.yg.gqlwfdl

import com.coxautodev.graphql.tools.SchemaParser
import com.coxautodev.graphql.tools.SchemaParserOptions
import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.dataloaders.DataLoaderFactory
import com.yg.gqlwfdl.resolvers.*
import com.yg.gqlwfdl.services.CompanyPartnershipService
import com.yg.gqlwfdl.services.CompanyService
import com.yg.gqlwfdl.services.CustomerService
import graphql.ExecutionInput
import graphql.ExecutionInput.newExecutionInput
import graphql.ExecutionResult
import graphql.GraphQL
import graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation
import graphql.schema.GraphQLSchema
import org.dataloader.DataLoader
import org.dataloader.DataLoaderRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse.badRequest
import org.springframework.web.reactive.function.server.ServerResponse.ok
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.*
import java.net.URLDecoder

private val GraphQLMediaType = MediaType.parseMediaType("application/GraphQL")

/**
 * Manages setting up the GraphQL entry points and serving them. Also responsible for reading the GraphQL schema
 * and setting up the GraphQL environment.
 */
@Configuration
class GraphQLRoutes(customerService: CustomerService,
                    companyService: CompanyService,
                    companyPartnershipService: CompanyPartnershipService,
                    private val dataLoaderFactory: DataLoaderFactory,
                    dbConfig: DBConfig) {

    private val schema = buildSchema(customerService, companyService, companyPartnershipService, dbConfig)

    /**
     * Sets up the routes (i.e. handles GET and POST to /graphql, and also serves up the graphiql HTML page).
     */
    @Bean
    fun router() = router {
        GET("/", serveStatic(ClassPathResource("/graphiql.html")))
        (POST("/graphql") or GET("/graphql")).invoke { req ->
            getGraphQLParameters(req)
                    .flatMap { executeGraphQLQuery(it) }
                    .flatMap { ok().syncBody(it) }
                    .switchIfEmpty(badRequest().build())
        }
    }

    /**
     * Handles an individual GraphQL request. Sets up the [ExecutionInput] and [GraphQL] objects and processes them
     * asynchronously by calling [GraphQL.executeAsync]. Also sets up all the [DataLoader]s and registers them so
     * they're available for this whole request. The setup performed below is inexpensive (as opposed to parsing the
     * schema file and building the schema, which is expensive), so it can be done at the start of each request.
     * This is how request-scoped data loaders can be defined.
     */
    private fun executeGraphQLQuery(graphQLParameters: GraphQLParameters): Mono<ExecutionResult> {
        // Create a data loader registry and register all the data loader into it. Wrap this in a RequestContext
        // object, and make this available to all the resolvers via the execution input's "context" property.
        val registry = DataLoaderRegistry()
        val requestContext = RequestContext(registry)
        dataLoaderFactory.createAllAndRegister(registry, requestContext)

        val executionInput = newExecutionInput()
                .query(graphQLParameters.query)
                .operationName(graphQLParameters.operationName)
                .variables(graphQLParameters.variables)
                // By putting an object into the execution input's context, the GraphQL resolvers will be able to
                // access this object (it's an optional parameter into all resolver methods, e.g. Query.customers).
                // That's the way that we make the data loaders available to the resolvers: they can get them from
                // the object in the context.  Note that any arbitrary object can be put into context.  In most
                // examples it's the DataLoaderRegistry which is put in here.  However to allow for more data to be
                // added in future if required, here we wrap the registry in a custom object, RequestContext.
                .context(requestContext)

        val graphQL = GraphQL
                .newGraphQL(schema)
                .instrumentation(DataLoaderDispatcherInstrumentation(registry))
                .build()

        return fromFuture(graphQL.executeAsync(executionInput))
    }
}

private fun getGraphQLParameters(req: ServerRequest): Mono<GraphQLParameters> = when {
    req.queryParam("query").isPresent -> graphQLParametersFromRequestParameters(req)
    req.method() == HttpMethod.POST -> parsePostRequest(req)
    else -> empty()
}

private fun parsePostRequest(req: ServerRequest) = when {
    req.contentTypeIs(GraphQLMediaType) -> req.withBody { GraphQLParameters(query = it) }
    else -> req.withBody { readJson<GraphQLParameters>(it) }
}

private fun graphQLParametersFromRequestParameters(req: ServerRequest) = just(
        GraphQLParameters(
                query = req.queryParam("query").get(),
                operationName = req.queryParam("operationName").orElseGet { null },
                variables = getVariables(req)
        )
)

private fun getVariables(req: ServerRequest): Map<String, Any>? {
    return req.queryParam("variables")
            .map { URLDecoder.decode(it, "UTF-8") }
            .map { readJsonMap(it) }
            .orElseGet { null }
}

/**
 * Reads the GraphQL schema (resources/schema.graphqls), sets up the resolvers for it, builds it, and returns it.
 * This operation can be expensive so is done once, on application startup.
 */
private fun buildSchema(customerService: CustomerService,
                        companyService: CompanyService,
                        companyPartnershipService: CompanyPartnershipService,
                        dbConfig: DBConfig): GraphQLSchema {

    return SchemaParser.newParser()
            .file("schema.graphqls")
            .resolvers(
                    Query(customerService, companyService, companyPartnershipService),
                    CustomerResolver(),
                    CompanyResolver(),
                    PricingDetailsResolver(),
                    Mutation(dbConfig))
            .options(SchemaParserOptions.newOptions()
                    .genericWrappers(SchemaParserOptions.GenericWrapper(Mono::class.java, 0))
                    .build())
            .build()
            .makeExecutableSchema()
}

private data class GraphQLParameters(
        val query: String,
        val operationName: String? = null,
        val variables: Map<String, Any>? = null
)
