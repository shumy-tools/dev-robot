package dr.io

import dr.schema.*
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

class DEntity(
  val schema: SEntity,
  private val mEntity: Map<String, Any?>? = null,
  private val cEntity: Any? = null    // may be Pack<Any>
) {
  init {
    if (mEntity == null && cEntity == null)
      throw Exception("Both values null! - (mEntity and cEntity)")

    if (mEntity != null && cEntity != null)
      throw Exception("Both values non-null! - (mEntity and cEntity)")
  }

  fun name() = schema.name

  fun unpack(): Pair<DEntity, List<DEntity>> {
    if (cEntity == null)
      return Pair(this, emptyList())

    return if (cEntity is Pack<*>) {
      schema.checkInclusion("sealed", schema.sealed.keys, cEntity.tail)
      val dTail: List<DEntity> = cEntity.tail.map {
        val sEntity = schema.sealed.getValue(it.javaClass.canonicalName)
        DEntity(sEntity, cEntity = it)
      }

      Pair(DEntity(schema, cEntity = cEntity.head), dTail)
    } else {
      Pair(this, emptyList())
    }
  }

  fun allFields(): List<DField> {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get field from a Pack<*>, unpack first.")

    return schema.fields.mapNotNull {
      contains(it.key) { DField(it.value, this) }
    }
  }

  fun allOwnedReferences(): List<DOwnedReference> {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get reference from a Pack<*>, unpack first.")

    return schema.rels.filter{ it.value.type == RelationType.CREATE && !it.value.isCollection }.mapNotNull {
      contains(it.key) { DOwnedReference(it.value, this) }
    }
  }

  fun allLinkedReferences(): List<DLinkedReference> {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get reference from a Pack<*>, unpack first.")

    return schema.rels.filter{ it.value.type == RelationType.LINK && !it.value.isCollection }.mapNotNull {
      contains(it.key) { DLinkedReference(it.value, this) }
    }
  }

  fun allOwnedCollections(): List<DOwnedCollection> {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get collection from a Pack<*>, unpack first.")

    return schema.rels.filter{ it.value.type == RelationType.CREATE && it.value.isCollection }.mapNotNull {
      contains(it.key) { DOwnedCollection(it.value, this) }
    }
  }

  fun allLinkedCollections(): List<DLinkedCollection> {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get collection from a Pack<*>, unpack first.")

    return schema.rels.filter{ it.value.isCollection }.mapNotNull { (name, sRelation) ->
      contains(name) { DLinkedCollection(sRelation, this) }
    }
  }

  fun toMap(): Map<String, Any?> {
    val map = linkedMapOf<String, Any?>()
    val (head, tail) = unpack()

    map.putAll(entityToMap(head))
    tail.forEach { map.putAll(entityToMap(it)) }

    return map
  }

  internal fun getFieldValue(field: SField): Any? {
    return if (mEntity != null) mEntity[field.name] else field.getValue(cEntity!!)
  }

  internal fun getOwnedRelationValue(rel: SRelation): Any? {
    return if (mEntity != null) mEntity[rel.name] else rel.getValue(cEntity!!)
  }

  internal fun getLinkedRelationValue(rel: SRelation): LinkData {
    return if (mEntity != null) mEntity[rel.name] as LinkData else rel.getValue(cEntity!!).translate()
  }

  private fun <T: Any> contains(name: String, exec: () -> T?): T? {
    return if (mEntity != null) {
      if (mEntity.containsKey(name)) exec() else null
    } else {
      exec()
    }
  }

  private fun entityToMap(ent: DEntity): Map<String, Any?> {
    val map = linkedMapOf<String, Any?>()
    map.putAll(ent.allFields().map{ Pair(it.name(), it.value()) })

    map.putAll(ent.allOwnedReferences().map { Pair(it.name(), it.value().toMap()) })
    map.putAll(ent.allLinkedReferences().map { Pair(it.name(), it.value()) })

    map.putAll(ent.allOwnedCollections().map {
      val cols = it.value().map { item -> Pair(item.name(), item.toMap()) }
      Pair(it.name(), cols)
    })

    map.putAll(ent.allLinkedCollections().map { Pair(it.name(), it.value()) })

    return map
  }
}

sealed class Data
  class DField(val schema: SField, private val entity: DEntity): Data() {
    fun name() = schema.name

    fun value(): Any? {
      val value = entity.getFieldValue(schema)

      // check field constraints
      return entity.schema.checkFieldConstraints(schema, value)
    }
  }

  sealed class DRelation: Data() {
    abstract val schema: SRelation
    abstract val entity: DEntity

    fun name() = schema.name
  }

    sealed class DReference: DRelation()

      class DOwnedReference(override val schema: SRelation, override val entity: DEntity): DReference() {
        fun value(): DEntity {
          val oneValue = entity.getOwnedRelationValue(schema)
          return DEntity(schema.ref, cEntity = oneValue)
        }
      }

      class DLinkedReference(override val schema: SRelation, override val entity: DEntity): DReference() {
        fun value(): OneLink {
          val value = entity.getLinkedRelationValue(schema)

          // check existing traits
          if (value is OneLinkWithTraits)
            entity.schema.checkInclusion(schema.name, schema.traits.keys, value.ref.traits)

          return value as OneLink
        }
      }

    sealed class DCollection: DRelation()

      class DOwnedCollection(override val schema: SRelation, override val entity: DEntity): DCollection() {
        @Suppress("UNCHECKED_CAST")
        fun value(): List<DEntity> {
          val manyValues = entity.getOwnedRelationValue(schema)
          return (manyValues as Collection<Any>).map { DEntity(schema.ref, cEntity = it) }
        }
      }

      class DLinkedCollection(override val schema: SRelation, override val entity: DEntity): DCollection() {
        fun value(): LinkData {
          val value = entity.getLinkedRelationValue(schema)

          // check existing traits (one-link-traits)
          if (value is OneLinkWithTraits)
            entity.schema.checkInclusion(schema.name, schema.traits.keys, value.ref.traits)

          // check existing traits (many-links-traits)
          if (value is ManyLinksWithTraits)
            value.refs.forEach { entity.schema.checkInclusion(schema.name, schema.traits.keys, it.traits) }

          return value
        }
      }


/* ------------------------- helpers -------------------------*/
private fun SEntity.checkFieldConstraints(sField: SField, value: Any?): Any? {
  if (value != null)
    for (constraint in sField.checks)
      constraint.check(value)?.let { throw Exception("Constraint check failed '$it'! - (${this.name}, ${sField.name})") }

  return value
}

private fun SEntity.checkInclusion(prop: String, expected: Set<String>, inputs: List<Any>) {
  if (expected.size != inputs.size)
    throw Exception("Invalid number of inputs, expected '${expected.size}' found '${inputs.size}'! - (${this.name}, $prop)")

  val processed = mutableSetOf<String>()
  for (input in inputs) {
    val nInput = input.javaClass.kotlin.qualifiedName!!
    processed.add(nInput)

    if (!expected.contains(nInput))
      throw Exception("Input type '$nInput' is not part of the model! - (${this.name}, $prop)")
  }

  if (expected.size != processed.size)
    throw Exception("Invalid number of inputs, expected '${expected.size}' found '${inputs.size}'! - (${this.name}, $prop)")
}

@Suppress("UNCHECKED_CAST")
private fun Any?.translate(): LinkData {
  return when (val data = this!!) {
    is Long -> OneLinkWithoutTraits(data)
    is Traits -> OneLinkWithTraits(data)
    is Collection<*> -> {
      if (data.isEmpty()) ManyLinksWithoutTraits() else {
        if (data.first() is Long)
          ManyLinksWithoutTraits(data as Collection<Long>)
        else
          ManyLinksWithTraits(data as Collection<Traits>)
      }
    }
    else -> throw  Exception("Unable to translate link '${data.javaClass.kotlin.qualifiedName}'! - (Code bug, please report the issue)")
  }
}