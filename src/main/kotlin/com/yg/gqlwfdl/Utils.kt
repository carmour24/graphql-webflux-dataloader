package com.yg.gqlwfdl

// Miscellaneous utility functions

/**
 * If the passed in [Iterable] has [any][Iterable.any] values in it, this calls the specified function [block] with `this`
 * value as its argument and returns its result. Otherwise returns null.
 *
 * @param T The type of objects being iterated over.
 * @param R The result of the passed in block.
 * @param I The type of [Iterable] (e.g. a [List]).
 * @see [let]
 */
inline fun <T, R, I : Iterable<T>> I.letIfAny(block: (I) -> R): R? = if (this.any()) block(this) else null