package com.yg.gqlwfdl.dataaccess

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgPoolOptions
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Configuration object providing beans related to connecting to the Postgres database.
 *
 * @property dbConfig The database configuration information, used to get the connection details.
 */
@Configuration
class PGConnectionConfig(private val dbConfig: DBConfig) {

    /**
     * The options used to connect to the Postgres database.
     */
    @Bean
    fun pgOptions(): PgPoolOptions =
            PgPoolOptions.fromUri(dbConfig.url.substringAfter("jdbc:"))
                    .setUser(dbConfig.username)
                    .setPassword(dbConfig.password)
                    .setMaxSize(dbConfig.connectionPoolSize)

    /**
     * The Postgres database connection pool.
     */
    @Bean
    fun connectionPool(): PgPool = PgClient.pool(pgOptions())
}