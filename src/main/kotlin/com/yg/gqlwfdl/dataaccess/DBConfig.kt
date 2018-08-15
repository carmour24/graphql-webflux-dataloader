package com.yg.gqlwfdl.dataaccess

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

/**
 * Object providing configuration information related to working with the database (e.g. the URL to access it). The data
 * for this object is read from the application.properties file.
 *
 * @property url The URL used to connect to the database.
 * @property username The user name to use to connect to the database.
 * @property password The password to use to connect to the database.
 * @property connectionPoolSize The size of the connection pool to the database, i.e. the maximum number of database
 * connections that can be open at any point time.
 */
@Configuration
class DBConfig(@param:Value("\${spring.datasource.url}") val url: String,
               @param:Value("\${spring.datasource.username}") val username: String,
               @param:Value("\${spring.datasource.password}") val password: String,
               @param:Value("\${spring.datasource.max-active}") val connectionPoolSize: Int)