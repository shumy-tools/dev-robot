package dr.io

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import dr.schema.*
import dr.schema.tabular.STATE
import dr.schema.tabular.SUPER
import dr.schema.tabular.TYPE

class DEntity(
  val schema: SEntity,
  private val mEntity: Map<String, Any?>? = null,
  private val cEntity: Any? = null    // may be Pack<Any>
) {
  private val sv = mutableMapOf<String, Any>() // TYPE, STATE, SUPER

  val unpack: Pair<DEntity, List<DEntity>>

  init {
    if (mEntity == null && cEntity == null)
      throw Exception("Both values null! - (mEntity and cEntity)")

    if (mEntity != null && cEntity != null)
      throw Exception("Both values non-null! - (mEntity and cEntity)")

    if (cEntity != null) {
      // call @LateInit if exists
      schema.initFun?.call(cEntity)

      // set initial machine state
      if (schema.machine != null)
        sv[STATE] = schema.machine.states.first()
    }

    // unpack
    unpack = if (cEntity != null && cEntity is Pack<*>) {
      val dHead = DEntity(schema, cEntity = cEntity.head)

      var topEntity = dHead
      val dTail: List<DEntity> = cEntity.tail.map { nxt ->
        if (!topEntity.schema.isSealed)
          throw Exception("Not a top @Sealed entity! - (${topEntity.name})")

        val sType = nxt.javaClass.canonicalName
        val sEntity = topEntity.schema.sealed[sType] ?: throw Exception("SubEntity '$sType' is not part of ${topEntity.name}!")
        DEntity(sEntity, cEntity = nxt).also {
          topEntity.sv[TYPE] = it.name
          it.sv[SUPER] = topEntity
          topEntity = it
        }
      }

      Pair(dHead, dTail)
    } else {
      Pair(this, emptyList())
    }
  }

  val name: String
    get() = schema.name

  val superRef: DEntity?
    get() = sv[SUPER] as DEntity?

  val allFields: List<DField> by lazy {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get field from a Pack<*>, unpack first.")

    schema.fields.mapNotNull {
      contains(it.key) { DField(it.value, this) }
    }
  }

  val allOwnedReferences: List<DOwnedReference> by lazy {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get reference from a Pack<*>, unpack first.")

    schema.allOwnedReferences.mapNotNull {
      contains(it.key) { DOwnedReference(it.value, this) }
    }
  }

  val allLinkedReferences: List<DLinkedReference> by lazy {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get reference from a Pack<*>, unpack first.")

    schema.allLinkedReferences.mapNotNull {
      contains(it.key) { DLinkedReference(it.value, this) }
    }
  }

  val allOwnedCollections: List<DOwnedCollection> by lazy {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get collection from a Pack<*>, unpack first.")

    schema.allOwnedCollections.mapNotNull {
      contains(it.key) { DOwnedCollection(it.value, this) }
    }
  }

  val allLinkedCollections: List<DLinkedCollection> by lazy {
    if (cEntity is Pack<*>)
      throw Exception("Cannot get collection from a Pack<*>, unpack first.")

    schema.allLinkedCollections.mapNotNull {
      contains(it.key) { DLinkedCollection(it.value, this) }
    }
  }

  fun checkFields(): Map<String, List<String>> {
    val all = linkedMapOf<String, List<String>>()

    val (head, tail) = this.unpack
    head.allFields.forEach { val res = it.check(); if (res.isNotEmpty()) all[it.name] = res }
    tail.forEach { ent -> ent.allFields.forEach { val res = it.check(); if (res.isNotEmpty()) all[it.name] = res } }

    return all
  }

  fun toMap(): Map<String, Any?> {
    val map = linkedMapOf<String, Any?>()
    val (head, tail) = unpack

    map.putAll(entityToMap(head))
    tail.forEach { map.putAll(entityToMap(it)) }

    return map
  }

  internal fun getFieldValue(field: SField): Any? {
    return if (mEntity != null) mEntity[field.name] else when (field.name) {
      TYPE -> sv[TYPE]
      STATE -> sv[STATE]
      else -> field.getValue(cEntity!!)
    }
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
    map.putAll(ent.allFields.map{ Pair(it.name, it.value) })

    map.putAll(ent.allOwnedReferences.map { Pair(it.name, it.value.toMap()) })
    map.putAll(ent.allLinkedReferences.map { Pair(it.name, it.value) })

    map.putAll(ent.allOwnedCollections.map {
      val cols = it.value.map { item -> Pair(item.name, item.toMap()) }
      Pair(it.name, cols)
    })

    map.putAll(ent.allLinkedCollections.map { Pair(it.name, it.value) })

    return map
  }
}

sealed class Data
  class DField(val schema: SField, private val entity: DEntity): Data() {
    val name: String
      get() = schema.name

    val value: Any? by lazy {
      val value = entity.getFieldValue(schema)
      val checks = entity.schema.checkFieldConstraints(schema, value)
      if (checks.isNotEmpty())
        throw Exception("Constraint check failed '${checks.first()}'! - (${entity.schema.name}, ${schema.name})")

      value
    }

    fun check(): List<String> {
      val value = entity.getFieldValue(schema)
      return entity.schema.checkFieldConstraints(schema, value)
    }
  }

  sealed class DRelation: Data() {
    abstract val schema: SRelation
    abstract val entity: DEntity

    val name: String
      get() = schema.name
  }

    sealed class DReference: DRelation()

      class DOwnedReference(override val schema: SRelation, override val entity: DEntity): DReference() {
        val value: DEntity by lazy {
          val oneValue = entity.getOwnedRelationValue(schema)
          DEntity(schema.ref, cEntity = oneValue)
        }
      }

      class DLinkedReference(override val schema: SRelation, override val entity: DEntity): DReference() {
        val value: OneLink by lazy {
          val value = entity.getLinkedRelationValue(schema)

          // check existing traits
          if (value is OneLinkWithTraits)
            entity.schema.checkInclusion(schema.name, schema.traits.keys, value.ref.traits)

          value as OneLink
        }
      }

    sealed class DCollection: DRelation()

      class DOwnedCollection(override val schema: SRelation, override val entity: DEntity): DCollection() {
        @Suppress("UNCHECKED_CAST")
        val value: List<DEntity> by lazy {
          val manyValues = entity.getOwnedRelationValue(schema)
          (manyValues as Collection<Any>).map { DEntity(schema.ref, cEntity = it) }
        }
      }

      class DLinkedCollection(override val schema: SRelation, override val entity: DEntity): DCollection() {
        val value: LinkData by lazy {
          val value = entity.getLinkedRelationValue(schema)

          // check existing traits (one-link-traits)
          if (value is OneLinkWithTraits)
            entity.schema.checkInclusion(schema.name, schema.traits.keys, value.ref.traits)

          // check existing traits (many-links-traits)
          if (value is ManyLinksWithTraits)
            value.refs.forEach { entity.schema.checkInclusion(schema.name, schema.traits.keys, it.traits) }

          value
        }
      }

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class LinkData

  sealed class ManyLinks: LinkData()

    @JsonTypeName("many-links")
    data class ManyLinksWithoutTraits(val refs: Collection<RefID>): ManyLinks() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

    @JsonTypeName("many-links-traits")
    data class ManyLinksWithTraits(val refs: Collection<Traits>): ManyLinks() {
      constructor(vararg refs: Traits): this(refs.toList())
    }

    @JsonTypeName("many-unlink")
    data class ManyUnlink(val refs: Collection<RefID>): ManyLinks() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

  sealed class OneLink: LinkData()

    @JsonTypeName("one-link")
    data class OneLinkWithoutTraits(val ref: RefID): OneLink()

    @JsonTypeName("one-link-traits")
    data class OneLinkWithTraits(val ref: Traits): OneLink()

    @JsonTypeName("one-unlink")
    data class OneUnlink(val ref: RefID): OneLink()


/* ------------------------- helpers -------------------------*/
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
    is RefID -> OneLinkWithoutTraits(data)
    is Traits -> OneLinkWithTraits(data)
    is Collection<*> -> {
      if (data.isEmpty()) ManyLinksWithoutTraits() else {
        if (data.first() is RefID)
          ManyLinksWithoutTraits(data as Collection<RefID>)
        else
          ManyLinksWithTraits(data as Collection<Traits>)
      }
    }
    else -> throw  Exception("Unable to translate link '${data.javaClass.kotlin.qualifiedName}'! - (Code bug, please report the issue)")
  }
}