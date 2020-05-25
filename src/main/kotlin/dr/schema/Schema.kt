package dr.schema

import dr.state.Machine
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1

enum class EntityType {
  MASTER, DETAIL, TRAIT
}

enum class RelationType {
  OWN, LINK
}

class Schema {
  val entities: Map<String, SEntity> = linkedMapOf()
  val masters: Map<String, SEntity> = linkedMapOf()
  val traits: Map<String, SEntity> = linkedMapOf()

  fun find(clazz: KClass<out Any>): SEntity {
    val name = clazz.qualifiedName
    return this.entities[name] ?: throw Exception("Entity type not found! - ($name)")
  }

  fun findClass(entity: String): KClass<out Any> {
    val sEntity = entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    return sEntity.clazz
  }

  internal fun addEntity(sEntity: SEntity) {
    (entities as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
    when (sEntity.type) {
      EntityType.MASTER -> (masters as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
      EntityType.TRAIT -> (traits as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
      else -> Unit
    }
  }

  fun toMap(simple: Boolean): Map<String, Any> {
    val map = linkedMapOf<String, Any>()
    map["masters"] = masters.map { it.key to it.value.toMap(simple) }.toMap()
    if (traits.isNotEmpty()) map["traits"] = traits.map { it.key to it.value.toMap(simple) }.toMap()

    return map
  }
}

  /* ------------------------- entity -------------------------*/
  class SEntity(val clazz: KClass<out Any>, val type: EntityType, val isSealed: Boolean, val initFun: KFunction<*>?) {
    val sealed: Map<String, SEntity> = linkedMapOf()
    val fields: Map<String, SField> = linkedMapOf()
    val rels: Map<String, SRelation> = linkedMapOf()

    var machine: SMachine? = null
      internal set

    val name: String
      get() = clazz.qualifiedName!!

    val allOwnedReferences: Map<String, SRelation> by lazy {
      rels.filter { it.value.type == RelationType.OWN && !it.value.isCollection }
    }

    val allLinkedReferences: Map<String, SRelation> by lazy {
      rels.filter { it.value.type == RelationType.LINK && !it.value.isCollection }
    }

    val allOwnedCollections: Map<String, SRelation> by lazy {
      rels.filter { it.value.type == RelationType.OWN && it.value.isCollection }
    }

    val allLinkedCollections: Map<String, SRelation> by lazy {
      rels.filter { it.value.type == RelationType.LINK && it.value.isCollection }
    }

    fun getFieldOrRelation(name: String): SFieldOrRelation? {
      return fields[name] ?: rels[name]
    }

    fun checkFieldConstraints(sField: SField, value: Any?): List<String> {
      return if (value != null)
        sField.checks.mapNotNull { it.check(value) }
      else
        emptyList()
    }

    internal fun addSealed(name: String, sEntity: SEntity) {
      (sealed as LinkedHashMap<String, SEntity>)[name] = sEntity
    }

    internal fun addProperty(name: String, prop: SFieldOrRelation) {
      when (prop) {
        is SField -> (fields as LinkedHashMap<String, SField>)[name] = prop
        is SRelation -> (rels as LinkedHashMap<String, SRelation>)[name] = prop
      }
    }

    fun toMap(simple: Boolean): Map<String, Any> {
      val map = linkedMapOf<String, Any>()
      if(!simple) map[TYPE] = type.name.toLowerCase()
      if (sealed.isNotEmpty()) map["sealed"] = sealed.map { it.key to it.value.toMap(simple) }.toMap()
      if (fields.isNotEmpty()) map["fields"] = fields.filter { if (simple) it.value.isInput && !it.value.isOptional else true }.map { it.key to it.value.toMap(simple) }.toMap()
      if (rels.isNotEmpty()) map["rels"] = rels.filter { if (simple) it.value.isInput && !it.value.isOptional else true }.map { it.key to it.value.toMap(simple) }.toMap()

      return map
    }

    override fun toString() = name
  }

    class SMachine(val clazz: KClass<out Machine<*, *, *>>, val states: Map<String, Enum<*>>, val events: Map<String, KClass<out Any>>) {
      val name: String
        get() = clazz.qualifiedName!!

      override fun toString() = name
    }


    sealed class SFieldOrRelation(val name: String, private val property: KProperty1<Any, *>?, val isOptional: Boolean) {
      var isInput: Boolean = false
        internal set

      var isUnique: Boolean = false
        internal set

      fun getValue(instance: Any): Any? = property?.get(instance)
    }

      class SField (
        name: String,
        property: KProperty1<Any, *>?,
        val type: FieldType,
        val checks: Set<SCheck>,

        isOptional: Boolean
      ): SFieldOrRelation(name, property, isOptional) {
        val jType: Class<out Any> by lazy {
          when (type) {
            FieldType.TEXT -> java.lang.String::class.java
            FieldType.INT -> java.lang.Integer::class.java
            FieldType.LONG -> java.lang.Long::class.java
            FieldType.FLOAT -> java.lang.Float::class.java
            FieldType.DOUBLE -> java.lang.Double::class.java
            FieldType.BOOL -> java.lang.Boolean::class.java

            FieldType.TIME -> LocalTime::class.java
            FieldType.DATE -> LocalDate::class.java
            FieldType.DATETIME -> LocalDateTime::class.java
            FieldType.MAP -> java.lang.String::class.java
            FieldType.LIST -> java.lang.String::class.java
            FieldType.SET -> java.lang.String::class.java
          }
        }

        fun toMap(simple: Boolean): Map<String, Any> {
          val map = linkedMapOf<String, Any>()
          map[TYPE] = type.name.toLowerCase()
          if(!simple) map["input"] = isInput
          if(!simple) map["optional"] = isOptional
          if(!simple) map["unique"] = isUnique

          return map
        }

        override fun toString() = name
      }

      class SRelation (
        name: String,
        property: KProperty1<Any, *>?,
        val type: RelationType,
        val ref: SEntity,
        val traits: Map<String, SEntity>,

        val isCollection: Boolean,
        val isOpen: Boolean,
        isOptional: Boolean
      ): SFieldOrRelation(name, property, isOptional) {
        fun toMap(simple: Boolean): Map<String, Any> {
          val map = linkedMapOf<String, Any>()
          map[TYPE] = type.name.toLowerCase()
          if(!simple) map["input"] = isInput
          if(!simple) map["optional"] = isOptional
          if(!simple) map["unique"] = isUnique
          map["many"] = isCollection
          if (traits.isNotEmpty()) map["traits"] = traits.map { it.key to it.key }.toMap()
          map["ref"] = if (type == RelationType.OWN) ref.toMap(simple) else ref.name

          return map
        }
      }

      class SCheck(val name: String, private val check: FieldCheck<Any>) {
        fun check(value: Any): String? = this.check.check(value)

        override fun toString() = name
      }

/* ----------- Helper printer functions ----------- */
const val SPACES = 3

fun Schema.print(filter: String = "all") {
  val tab = " ".repeat(SPACES)

  println("-------SCHEMA (filter=$filter)-------")
  val mFiltered = if (filter == "all") this.masters else this.masters.filter{ (key, _) -> key.startsWith(filter) }
  val mTraits = if (filter == "all") this.traits else this.traits.filter{ (key, _) -> key.startsWith(filter) }

  println("<MASTER>")
  for ((name, entity) in mFiltered) {
    val sealedText = if (entity.isSealed) " - SEALED" else ""
    println("$tab$name$sealedText")
    entity.print(SPACES + SPACES)
  }

  println("<TRAIT>")
  for ((name, trait) in mTraits) {
    println("  $name")
    trait.print(SPACES + SPACES)
  }
}

fun SEntity.print(spaces: Int) {
  val tab = " ".repeat(spaces)

  if (this.initFun != null)
    println("${tab}${initFun.name}() - INIT-FUN")

  this.fields.print(tab)
  this.rels.print(tab, spaces)

  for ((sName, sEntity) in this.sealed) {
    println("$tab@$sName")
    sEntity.print(spaces + SPACES)
  }
}

fun Map<String, SField>.print(tab: String) {
  for ((name, field) in this) {
    val isOptional = if (field.isOptional) "OPT " else ""
    val isInput = if (field.isInput) "IN-" else "DER-"

    val checks = field.checks.map{ it.name }
    val sChecks = if (checks.isEmpty()) "" else " CHECKS-$checks"
    println("${tab}$name: ${field.type} - ${isOptional}${isInput}FIELD${sChecks}")
  }
}

fun Map<String, SRelation>.print(tab: String, spaces: Int) {
  for ((name, rel) in this) {
    val isOpen = if (rel.isOpen) "OPEN " else ""
    val isOptional = if (rel.isOptional) "OPT " else ""
    val isInput = if (rel.isInput) "IN-" else "DER-"

    val traits = rel.traits.map{ it.key }
    val sTraits = if (traits.isEmpty()) "" else " TRAITS-$traits"
    println("${tab}$name: ${rel.ref.name} - ${isOpen}${isOptional}${isInput}${rel.type}${sTraits}")
    if (rel.ref.type != EntityType.MASTER) {
      rel.ref.print(spaces + SPACES)
    }
  }
}