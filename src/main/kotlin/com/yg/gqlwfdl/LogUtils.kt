package com.yg.gqlwfdl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture

// Miscellaneous logging-related methods

fun logMessage(message: String, includeThreadName: Boolean = true) {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSS"))
    val threadInfo = if (includeThreadName) " [${Thread.currentThread().name}]" else ""
    println("$timestamp$threadInfo :: $message")
}

// TODO: remove this eventually?  Replace with some sort of injecting logging framework?
fun <T> withLogging(taskDescription: String, handler: () -> CompletableFuture<T>): CompletableFuture<T> {
    logMessage("${taskDescription.capitalize()}...")
    return handler().whenComplete { result, error ->
        val stringBuilder = StringBuilder()
        val details = if (error == null) {
            stringBuilder.append("Success ")
            result.toString()
        } else {
            stringBuilder.append("Error ")
            error.toString()
        }
        stringBuilder.append(taskDescription).append(":")
        if (details.contains('\n'))
            stringBuilder.appendln()
        else
            stringBuilder.append(" ")
        stringBuilder.append(details)
        logMessage(stringBuilder.toString())
    }
}