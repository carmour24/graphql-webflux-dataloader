package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.services.Entity
import org.jooq.Record

/**
 * An object that looks at the joined records related to a given primary record and creates any [TEntity] objects that
 * can be created from the joined tables, potentially working with multiple joined tables if the entity requires data
 * from more than one table.
 *
 * @param TEntity The type of entity that this object converts data into.
 */
interface JoinedRecordToEntityConverter<TEntity : Entity<out Any>> {
    /**
     * Gets a list of entities from the passed in [joinResults]. Typically there will only be one entity for a row,
     * but in some cases there could be multiples (e.g. a table that joins two entities of the same type could have one
     * row that results in two entities being created).
     *
     * @param primaryRecord The primary record to which all the [joinResults] are related.
     * @param joinResults All the [JoinResult]s from the passed in [primaryRecord].
     */
    fun getEntities(primaryRecord: Record, joinResults: List<JoinResult<out Any, out Record, out Record>>): List<TEntity>
}

/**
 * An object that converts a single joined record into entities, where data from only one table is required. When
 * [getEntities] is called, this will look through every supplied join result and, if its [JoinResult.foreignRecord] is
 * of the same type as this object's [TRecord], it will convert that record into an [Entity]. So this object does look
 * at joined tables, but, when creating the entities, only requires the data from each individual joined table: not from
 * the primary table or any other tables.
 *
 * @param TRecord The type of record which the object converts into an entity.
 * @param TEntity The type of entity which this object converts the record into.
 * @property recordClass The [Class] of the joined record to convert into an entity.
 * @property converter The function to call to convert a record of type [TRecord] into an [Entity].
 */
class SingleTypeJoinedRecordToEntityConverter<TRecord : Record, TEntity : Entity<out Any>>(
        private val recordClass: Class<TRecord>,
        private val converter: (TRecord) -> TEntity) : JoinedRecordToEntityConverter<TEntity> {

    override fun getEntities(primaryRecord: Record, joinResults: List<JoinResult<out Any, out Record, out Record>>)
            : List<TEntity> {

        // Look at all the join results and, for each one, check if its foreignRecord is of the same type as this
        // object's TRecord (recordClass). If so, call the "converter" method to convert that record to an entity.
        return joinResults.mapNotNull {
            if (recordClass.isInstance(it.foreignRecord)) {
                // Can safely cast here because of isInstance check above.
                @Suppress("UNCHECKED_CAST")
                converter(it.foreignRecord as TRecord)
            } else null
        }
    }
}

/**
 * An object that converts two or more joined records into entities, where data from more than one table is required.
 * When [getEntities] is called, this will look through the [requiredJoins] and see if all the required joins are
 * contained within the passed in [JoinResult]s. If so, it will then convert those records into an [Entity].
 *
 * This is an abstract base class: there is a concrete subclass for the different numbers of joined tables, or for
 * specific instances where a more complex join hierarchy is required.
 *
 * @param TPrimaryRecord The type of the primary record, to which all the [JoinResult]s are related.
 * @param TEntity The type of entity which this object converts the record into.
 * @property primaryRecordClass The [Class] of the [TPrimaryRecord].
 * @property requiredJoins A map of the joins which are required by this object in order for it to be able to convert
 * the joined data into the entity. The map is keyed on the join name, and the value is the class of the record which
 * is expected at the foreign side of the join.
 */
abstract class MultiTypeJoinedRecordToEntityConverter<TPrimaryRecord : Record, TEntity : Entity<out Any>>(
        private val primaryRecordClass: Class<TPrimaryRecord>,
        private val requiredJoins: Map<String, Class<out Record>>)
    : JoinedRecordToEntityConverter<TEntity> {

    override fun getEntities(primaryRecord: Record, joinResults: List<JoinResult<out Any, out Record, out Record>>)
            : List<TEntity> {

        // Check if the primary record is the type we're interested in and, if so, get a map of the join results which
        // corresponds to the required joins. If that gets a value it means we have all the joins we need, so continue
        // and call the "convert" function to convert the joined records into the entities.
        return if (primaryRecordClass.isInstance(primaryRecord)) {
            joinResults.toMapOfAllMatching()?.let {
                // Can safely cast here because of isInstance check toMapOfAllMatching called above.
                @Suppress("UNCHECKED_CAST")
                listOf(convert(primaryRecord as TPrimaryRecord, it))
            } ?: listOf()
        } else listOf()
    }

    /**
     * Converts the [joinedRecords] to a [TEntity].
     *
     * @param primaryRecord The primary record to which all the [joinedRecords] are related.
     * @param joinedRecords A map containing the joined records which match this object's [requiredJoins]. This map is
     * keyed on the join name, and the value is the record at the foreign end of that join. That object can safely be
     * cast to the relevant type of record.
     */
    abstract fun convert(primaryRecord: Record, joinedRecords: Map<String, Record>): TEntity

    /**
     * Converts the receiver (a list of [JoinResult]s) to a map which corresponds to this object's required joins.
     * Interrogates the join results to see if they contain all those specified in [requiredJoins] and, if so, converts
     * them to a map keyed on the join name, where the value is the record at the foreign end of the join. If the join
     * results don't contain all the required joins null is returned.
     */
    private fun List<JoinResult<out Any, out Record, out Record>>.toMapOfAllMatching(): Map<String, Record>? {

        // TODO: rather than keying things by def name, use the actual join def object?
        // TODO: should any more of the join classes be data classes?   currently just join def and join result

        // Get the matching entry from the receiver, for each of the required joins.
        val matchingJoins = requiredJoins.mapNotNull { requiredJoin ->
            this.firstOrNull { it.definition.name == requiredJoin.key && requiredJoin.value.isInstance(it.foreignRecord) }
        }

        // If sizes match then we found them all: convert it to a map mapping the join name to the foreign record.
        // Otherwise return null.
        return if (matchingJoins.size == requiredJoins.size)
            matchingJoins.map { it.definition.name to it.foreignRecord }.toMap()
        else null
    }
}

/**
 * Concrete implementation of [MultiTypeJoinedRecordToEntityConverter] for when a single join is required from the
 * primary record.
 *
 * @param TForeignRecord The type of the foreign record required to create the entity.
 * @property joinName The name of the join to the [TForeignRecord].
 * @param foreignRecordClass The [Class] of the [TForeignRecord].
 * @property converter A function which will receive the primary record and joined foreign record, and which should
 * return the [TEntity] object based on the data in those records.
 */
class MultiTypeJoinedRecordToEntityConverter1<
        TPrimaryRecord : Record, TForeignRecord : Record, TEntity : Entity<out Any>>(
        primaryRecordClass: Class<TPrimaryRecord>,
        private val joinName: String,
        foreignRecordClass: Class<TForeignRecord>,
        private val converter: (TPrimaryRecord, TForeignRecord) -> TEntity)
    : MultiTypeJoinedRecordToEntityConverter<TPrimaryRecord, TEntity>(
        primaryRecordClass, mapOf(Pair(joinName, foreignRecordClass))) {

    override fun convert(primaryRecord: Record, joinedRecords: Map<String, Record>): TEntity {
        // Can safely cast here because we know that the joinedRecords map has the right types.
        @Suppress("UNCHECKED_CAST")
        return converter(primaryRecord as TPrimaryRecord, joinedRecords[joinName] as TForeignRecord)
    }
}

/**
 * Concrete implementation of [MultiTypeJoinedRecordToEntityConverter] for when two joins are required from the
 * primary record.
 *
 * @param TForeignRecord1 The type of the first foreign record required to create the entity.
 * @param TForeignRecord2 The type of the second foreign record required to create the entity.
 * @property joinName1 The name of the first join to the [TForeignRecord1].
 * @property joinName2 The name of the second join to the [TForeignRecord2].
 * @param foreignRecordClass1 The [Class] of the [TForeignRecord1].
 * @param foreignRecordClass2 The [Class] of the [TForeignRecord2].
 * @property converter A function which will receive the primary record and the two joined foreign records, and which
 * should return the [TEntity] object based on the data in those records.
 */
class MultiTypeJoinedRecordToEntityConverter2<
        TPrimaryRecord : Record, TForeignRecord1 : Record, TForeignRecord2 : Record, TEntity : Entity<out Any>>(
        primaryRecordClass: Class<TPrimaryRecord>,
        private val joinName1: String,
        private val joinName2: String,
        foreignRecordClass1: Class<TForeignRecord1>,
        foreignRecordClass2: Class<TForeignRecord2>,
        private val converter: (TPrimaryRecord, TForeignRecord1, TForeignRecord2) -> TEntity)
    : MultiTypeJoinedRecordToEntityConverter<TPrimaryRecord, TEntity>(
        primaryRecordClass,
        mapOf(
                Pair(joinName1, foreignRecordClass1),
                Pair(joinName2, foreignRecordClass2))) {

    override fun convert(primaryRecord: Record, joinedRecords: Map<String, Record>): TEntity {
        // Can safely cast here because we know that the joinedRecords map has the right types.
        @Suppress("UNCHECKED_CAST")
        return converter(primaryRecord as TPrimaryRecord,
                joinedRecords[joinName1] as TForeignRecord1,
                joinedRecords[joinName2] as TForeignRecord2)
    }
}

/**
 * Concrete implementation of [MultiTypeJoinedRecordToEntityConverter] for when three joins are required from the
 * primary record.
 *
 * @param TForeignRecord1 The type of the first foreign record required to create the entity.
 * @param TForeignRecord2 The type of the second foreign record required to create the entity.
 * @param TForeignRecord3 The type of the third foreign record required to create the entity.
 * @property joinName1 The name of the first join to the [TForeignRecord1].
 * @property joinName2 The name of the second join to the [TForeignRecord2].
 * @property joinName3 The name of the third join to the [TForeignRecord2].
 * @param foreignRecordClass1 The [Class] of the [TForeignRecord1].
 * @param foreignRecordClass2 The [Class] of the [TForeignRecord2].
 * @param foreignRecordClass3 The [Class] of the [TForeignRecord3].
 * @property converter A function which will receive the primary record and the three joined foreign records, and which
 * should return the [TEntity] object based on the data in those records.
 */
class MultiTypeJoinedRecordToEntityConverter3<
        TPrimaryRecord : Record, TForeignRecord1 : Record, TForeignRecord2 : Record, TForeignRecord3 : Record, TEntity : Entity<out Any>>(
        primaryRecordClass: Class<TPrimaryRecord>,
        private val joinName1: String,
        private val joinName2: String,
        private val joinName3: String,
        foreignRecordClass1: Class<TForeignRecord1>,
        foreignRecordClass2: Class<TForeignRecord2>,
        foreignRecordClass3: Class<TForeignRecord3>,
        private val converter: (TPrimaryRecord, TForeignRecord1, TForeignRecord2, TForeignRecord3) -> TEntity)
    : MultiTypeJoinedRecordToEntityConverter<TPrimaryRecord, TEntity>(
        primaryRecordClass,
        mapOf(
                Pair(joinName1, foreignRecordClass1),
                Pair(joinName2, foreignRecordClass2),
                Pair(joinName3, foreignRecordClass3))) {

    override fun convert(primaryRecord: Record, joinedRecords: Map<String, Record>): TEntity {
        // Can safely cast here because we know that the joinedRecords map has the right types.
        @Suppress("UNCHECKED_CAST")
        return converter(primaryRecord as TPrimaryRecord,
                joinedRecords[joinName1] as TForeignRecord1,
                joinedRecords[joinName2] as TForeignRecord2,
                joinedRecords[joinName3] as TForeignRecord3)
    }
}