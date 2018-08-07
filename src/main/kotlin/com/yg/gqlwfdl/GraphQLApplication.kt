package com.yg.gqlwfdl

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * The main Spring Boot application.
 */
@SpringBootApplication
class GraphQLApplication

/**
 * Main entry point.
 */
fun main(args: Array<String>) {
    runApplication<GraphQLApplication>(*args)
}