package com.yg.gqlwfdl.dataaccess

import io.reactiverse.pgclient.Row
import org.jooq.Field
import org.jooq.Record
import org.jooq.Table
import org.jooq.TableField

/**
 * Aggregates information about a query being executed, for example the instances of all the aliased tables. Provides
 * functionality for getting at the name to refer to a field by (its alias), based on the (aliased) table it belongs to.
 *
 * The need for storing the aliases rather than just calculating them on the fly in some way is caused by the fact that
 * there is a default 64 byte limit for field (and alias) names in Postgres (see
 * https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS), which can be reached
 * if enough tables are joined, so instead we assign shorter aliases and store them here.
 *
 * Additionally, when JOOQ generates SQL, it will generate SQL such as "SELECT A.X, B.X..." - i.e. two columns which have
 * the same column name. When we retrieve data from the query using the reactive-pg-client libraries, we do this by the
 * column name, so we need a unique name to be generated.
 *
 * @param sourceTable The the main table which the query starts with. This is immediately aliased and added to the list
 * of table instances in this query
 */
open class QueryInfo<TRecord : Record>(sourceTable: Table<TRecord>) {

    /**
     * The instance of the main table which the query starts with. This is aliased.
     */
    val primaryTable: Table<TRecord>

    /**
     * A map of all the (aliased) tables in the query, keyed on the table instance, where the value is the alias for
     * this table.
     */
    private val tablesByInstance: MutableMap<Table<out Record>, String> = mutableMapOf()

    /**
     * A map of all the (aliased) tables in the query, keyed on the alias, where the value is the table instance.
     */
    private val tablesByAlias: MutableMap<String, Table<out Record>> = mutableMapOf()

    init {
        // Alias the source table and store it as the primary table.
        primaryTable = addTable(sourceTable)
    }

    /**
     * Creates an aliased instance of the passed in [table] which is being added to this query, and adds it to the list
     * of table instances in the query.
     *
     * @param table The table to alias and add.
     * @param parentTable The parent table to which the passed in [table] was joined.
     * @param joinName The name of the join which caused the [table] to be joined. Used when generating an alias for the
     * table.
     * @param trimIfTooLong Whether to trim the alias if it's too long (i.e. longer than 4 characters). Defauls to true.
     * Set this to false if hard-coded aliases are being used, which shouldn't be trimmed.
     */
    fun <TRecord : Record> addJoinedTable(table: Table<TRecord>, parentTable: Table<out Record>, joinName: String,
                                          trimIfTooLong: Boolean = true)
            : Table<TRecord> {

        // Ensure the parent table exists in this query so far - if not, throw exception.
        return tablesByInstance[parentTable]?.let { tableAlias ->
            table.`as`(getUniqueAlias(tableAlias, joinName, trimIfTooLong)).also { it.addToMaps() }
        } ?: throw Exception("Parent table ${parentTable.name} not found in this query")
    }

    /**
     * Creates an aliased instance of the passed in [table], and adds it to the list of table instances in the query.
     *
     * @param table The table to alias and add.
     */
    private fun <TRecord : Record> addTable(table: Table<TRecord>): Table<TRecord> =
            table.`as`(table.getUniqueAlias()).also { it.addToMaps() }

    /**
     * Adds `this` to the maps of tables in this query.
     */
    private fun Table<out Record>.addToMaps() {
        tablesByInstance[this] = name
        tablesByAlias[name] = this
    }

    /**
     * Generates a unique alias (i.e. one that isn't already used in this query) for `this`.
     */
    private fun Table<out Record>.getUniqueAlias() = getUniqueAlias(getTruncatedAliasPart(name))

    /**
     * Generates a unique alias (i.e. one that isn't already used in this query) to use when joining a table to the
     * query.
     *
     * @param parentTableAlias The alias for the parent table to which the new table is being joined.
     * @param joinName The name of the join which caused the table to be added to the query.
     * @param trimIfTooLong Whether to trim the alias if it's too long (i.e. longer than 4 characters).
     */
    private fun getUniqueAlias(parentTableAlias: String, joinName: String, trimIfTooLong: Boolean) =
            getUniqueAlias(parentTableAlias + "_" + if (trimIfTooLong) getTruncatedAliasPart(joinName) else joinName)

    /**
     * Gets the value to use when generating an alias. Uses the passed in [aliasPart] unless it's longer than 4
     * characters, in which case only the first 4 characters are used.
     */
    private fun getTruncatedAliasPart(aliasPart: String) =
            if (aliasPart.length <= 4) aliasPart else aliasPart.substring(0, 4)

    /**
     * Gets a unique alias (i.e. one that isn't used already), defaulting to the passed in [defaultAlias]. If that value
     * is already used, keeps adding an incremental index to it till an unused one is found.
     */
    private fun getUniqueAlias(defaultAlias: String): String {
        var alias = defaultAlias
        var index = 1
        while (tablesByAlias.containsKey(alias)) {
            alias = defaultAlias + index++
        }
        return alias
    }

    /**
     * Gets all the fields in all the tables in this object, in their aliased form.
     */
    fun getAllFields() = tablesByAlias.flatMap { pair ->
        pair.value.fields().map { it.`as`(getAliasedFieldName(pair.key, it.name)) }
    }

    /**
     * Gets a list of the aliased [Field] instances from the passed in [table].
     *
     * @param table The table in this query, whose aliased fields are sought. If this table isn't in the query an
     * exception is thrown.
     */
    fun getAliasedFields(table: Table<out Record>) =
            tablesByInstance[table]?.let { aliasName ->
                table.fields().map { it.`as`(getAliasedFieldName(aliasName, it.name)) }
            } ?: throw Exception("Table ${table.name} not found in this query")

    /**
     * Gets the aliased field name to use for the passed in [field]. This field must come from a table which is in this
     * query, otherwise an exception is thrown.
     */
    fun getAliasedFieldName(field: TableField<out Record, out Any>) = getAliasedFieldName(field.table, field)

    /**
     * Gets the aliased field name to use for the passed in [field], from the passed in [table]. This table must exist
     * in this query, otherwise an exception is thrown.
     */
    private fun getAliasedFieldName(table: Table<out Record>, field: Field<out Any>) = getAliasedFieldName(table, field.name)

    /**
     * Gets the aliased field name to use for the passed in [fieldName], from the passed in [table]. This table must
     * exist in this query, otherwise an exception is thrown.
     */
    private fun getAliasedFieldName(table: Table<out Record>, fieldName: String) =
            tablesByInstance[table]?.let { getAliasedFieldName(it, fieldName) }
                    ?: throw Exception("Table ${table.name} not found in this query")

    /**
     * Gets the aliased field name to use for the passed in [fieldName] and [tableName].
     */
    private fun getAliasedFieldName(tableName: String, fieldName: String) = tableName + "_" + fieldName

    /**
     * Gets a string value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the string value in that field, or null if it has no such value (or no such field).
     */
    fun getNullableString(row: Row, table: Table<out Record>, field: Field<String>): String? =
            row.getString(getAliasedFieldName(table, field))

    /**
     * Gets a string value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the string value in that field, or thrown an exception if the value was null (or no such field exists).
     */
    fun getString(row: Row, table: Table<out Record>, field: Field<String>): String =
            getNullableString(row, table, field)!!

    /**
     * Gets a long value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the long value in that field, or null if it has no such value (or no such field).
     */
    fun getNullableLong(row: Row, table: Table<out Record>, field: Field<Long>): Long? =
            row.getLong(getAliasedFieldName(table, field))

    /**
     * Gets a long value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the long value in that field, or thrown an exception if the value was null (or no such field exists).
     */
    fun getLong(row: Row, table: Table<out Record>, field: Field<Long>): Long =
            getNullableLong(row, table, field)!!

    /**
     * Gets a double value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the double value in that field, or null if it has no such value (or no such field).
     */
    fun getNullableDouble(row: Row, table: Table<out Record>, field: Field<Double>): Double? =
            row.getDouble(getAliasedFieldName(table, field))

    /**
     * Gets a double value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the double value in that field, or thrown an exception if the value was null (or no such field exists).
     */
    fun getDouble(row: Row, table: Table<out Record>, field: Field<Double>): Double =
            getNullableDouble(row, table, field)!!

    /**
     * Gets an integer value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the integer value in that field, or null if it has no such value (or no such field).
     */
    fun getNullableInt(row: Row, table: Table<out Record>, field: Field<Int>): Int? =
            row.getInteger(getAliasedFieldName(table, field))

    /**
     * Gets an integer value from the passed in [row], reading it from the passed it [field], which came from the passed
     * in [table] (which must exist in this query, in order that the correct alias it was assigned can be found).
     *
     * Returns the integer value in that field, or thrown an exception if the value was null (or no such field exists).
     */
    fun getInt(row: Row, table: Table<out Record>, field: Field<Int>): Int =
            getNullableInt(row, table, field)!!
}