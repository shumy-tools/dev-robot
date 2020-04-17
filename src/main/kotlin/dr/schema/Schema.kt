package dr.schema

import com.fasterxml.jackson.annotation.JsonTypeInfo
import dr.io.Delete
import dr.io.Insert
import dr.io.Instruction
import dr.io.Update
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1

/* ------------------------- annotations -------------------------*/
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Sealed(vararg val value: KClass<out Any>)

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Master

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Detail

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Trait


@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Unique

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Open

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Create

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Link(val value: KClass<out Any>, vararg val traits: KClass<out Any>)


@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class LateInit

/* ------------------------- enums -------------------------*/
enum class EntityType {
  MASTER, DETAIL, TRAIT
}

enum class RelationType {
  CREATE, LINK
}

/* ------------------------- structures -------------------------*/
data class Pack<T: Any>(
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val head: T,

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val tail: List<Any>
) {
  constructor(head: T, vararg tail: Any): this(head, tail.toList())
}

data class Traits(
  val id: Long,

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val traits: List<Any>
) {
  constructor(id: Long, vararg traits: Any): this(id, traits.toList())
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
  class SEntity(val clazz: KClass<out Any>, val type: EntityType, val isSealed: Boolean, val initFun: KFunction<*>?, val listeners: Set<SListener>) {
    val sealed: Map<String, SEntity> = linkedMapOf()
    val fields: Map<String, SField> = linkedMapOf()
    val rels: Map<String, SRelation> = linkedMapOf()

    val name: String
      get() = clazz.qualifiedName!!

    fun getFieldOrRelation(name: String): SFieldOrRelation? {
      return fields[name] ?: rels[name]
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

    internal fun fireListeners(event: EventType, inst: Instruction) {
      inst.action.sEntity.listeners.forEach {
        when (inst.action.type) {
          ActionType.CREATE -> it.get(ActionType.CREATE, event)?.onCreate(inst as Insert)
          ActionType.UPDATE -> it.get(ActionType.UPDATE, event)?.onUpdate(inst as Update)
          ActionType.DELETE -> it.get(ActionType.DELETE, event)?.onDelete(inst as Delete)
          ActionType.ADD -> it.get(ActionType.ADD, event)?.onAdd(inst as Insert)
          ActionType.LINK -> it.get(ActionType.LINK, event)?.onLink(inst as Insert)
          ActionType.UNLINK -> it.get(ActionType.UNLINK, event)?.onUnlink(inst as Delete)
        }
      }
    }

    fun toMap(simple: Boolean): Map<String, Any> {
      val map = linkedMapOf<String, Any>()
      if(!simple) map["@type"] = type.name.toLowerCase()
      if (sealed.isNotEmpty()) map["sealed"] = sealed.map { it.key to it.value.toMap(simple) }.toMap()
      if (fields.isNotEmpty()) map["fields"] = fields.filter { if (simple) it.value.isInput && !it.value.isOptional else true }.map { it.key to it.value.toMap(simple) }.toMap()
      if (rels.isNotEmpty()) map["rels"] = rels.filter { if (simple) it.value.isInput && !it.value.isOptional else true }.map { it.key to it.value.toMap(simple) }.toMap()

      return map
    }
  }

    sealed class SFieldOrRelation(private val property: KProperty1<Any, *>, val isOptional: Boolean) {
      val name: String
        get() = property.name

      var isInput: Boolean = false
        internal set

      var isUnique: Boolean = false
        internal set

      fun getValue(instance: Any): Any? = this.property.get(instance)
    }

      class SField (
        property: KProperty1<Any, *>,
        val type: FieldType,
        val checks: Set<SCheck>,

        isOptional: Boolean
      ): SFieldOrRelation(property, isOptional) {
        fun toMap(simple: Boolean): Map<String, Any> {
          val map = linkedMapOf<String, Any>()
          map["@type"] = type.name.toLowerCase()
          if(!simple) map["input"] = isInput
          if(!simple) map["optional"] = isOptional
          if(!simple) map["unique"] = isUnique

          return map
        }
      }

      class SRelation (
        property: KProperty1<Any, *>,
        val type: RelationType,
        val ref: SEntity,
        val traits: Map<String, SEntity>,

        val isCollection: Boolean,
        val isOpen: Boolean,
        isOptional: Boolean
      ): SFieldOrRelation(property, isOptional) {
        fun toMap(simple: Boolean): Map<String, Any> {
          val map = linkedMapOf<String, Any>()
          map["@type"] = type.name.toLowerCase()
          if(!simple) map["input"] = isInput
          if(!simple) map["optional"] = isOptional
          if(!simple) map["unique"] = isUnique
          map["many"] = isCollection
          if (traits.isNotEmpty()) map["traits"] = traits.map { it.key to it.key }.toMap()
          map["ref"] = if (type == RelationType.CREATE) ref.toMap(simple) else ref.name

          return map
        }
      }

      class SCheck(val name: String, private val check: FieldCheck<Any>) {
        fun check(value: Any): String? = this.check.check(value)
      }

    @Suppress("UNCHECKED_CAST")
    class SListener(val name: String, internal val listener: EListener<*>, internal val enabled: Map<ActionType, Set<EventType>>) {
      internal fun get(action: ActionType, event: EventType): EListener<Any>? = enabled[action]?.let {
        if (it.contains(event)) listener as EListener<Any> else null
      }
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

  if (this.listeners.isNotEmpty()) {
    println("${tab}(listeners)")
    this.listeners.print(spaces + SPACES)
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

fun Set<SListener>.print(spaces: Int) {
  val tab = " ".repeat(spaces)
  for (lis in this) {
    val name = lis.listener.javaClass.kotlin.qualifiedName
    println("${tab}$name -> ${lis.enabled}")
  }
}