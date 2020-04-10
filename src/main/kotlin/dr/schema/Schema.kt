package dr.schema

import com.fasterxml.jackson.annotation.JsonTypeInfo
import dr.modification.Delete
import dr.modification.Insert
import dr.modification.Instruction
import dr.modification.Update
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

/* ------------------------- annotations -------------------------*/
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
annotation class Open

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Create

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Link(val value: KClass<out Any>, vararg val traits: KClass<out Any>)

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Sealed(vararg val value: KClass<out Any>)

/* ------------------------- enums -------------------------*/
enum class EntityType {
  MASTER, DETAIL, TRAIT
}

enum class RelationType {
  CREATE, LINK
}

/* ------------------------- structures -------------------------*/
class Pack(
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  vararg val values: Any
)

class Schema {
  val entities: Map<String, SEntity> = linkedMapOf()
  val masters: Map<String, SEntity> = linkedMapOf()
  val traits: Map<String, SEntity> = linkedMapOf()

  fun findEntity(value: Any): SEntity {
    val name = value.javaClass.kotlin.qualifiedName
    return this.entities[name] ?: throw Exception("Entity type not found! - ($name)")
  }

  internal fun addEntity(sEntity: SEntity) {
    (entities as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
    when (sEntity.type) {
      EntityType.MASTER -> (masters as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
      EntityType.TRAIT -> (traits as LinkedHashMap<String, SEntity>)[sEntity.name] = sEntity
      else -> Unit
    }
  }
}

  /* ------------------------- entity -------------------------*/
  class SEntity(val name: String, val type: EntityType, val isSealed: Boolean, val listeners: Set<SListener>) {
    val sealed: Map<String, SEntity> = linkedMapOf()
    val fields: Map<String, SField> = linkedMapOf()
    val rels: Map<String, SRelation> = linkedMapOf()

    internal fun addSealed(name: String, sEntity: SEntity) {
      (sealed as LinkedHashMap<String, SEntity>)[name] = sEntity
    }

    internal fun addProperty(name: String, prop: SFieldOrRelation) {
      when (prop) {
        is SField -> (fields as LinkedHashMap<String, SField>)[name] = prop
        is SRelation -> (rels as LinkedHashMap<String, SRelation>)[name] = prop
      }
    }

    fun fireListeners(event: EventType, inst: Instruction, id: Long? = null) {
      println("($event, $id) -> $inst")
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
  }

    sealed class SFieldOrRelation {
      abstract val name: String
      abstract val property: KProperty1<Any, *>
      abstract val isOptional: Boolean

      var isInput: Boolean = false
        internal set

      fun getValue(instance: Any): Any? {
        return this.property.get(instance)
      }
    }

      class SField (
        override val name: String,
        val type: FieldType,
        val checks: Set<SCheck>,
        override val property: KProperty1<Any, *>,

        override val isOptional: Boolean
      ): SFieldOrRelation()

      class SRelation (
        override val name: String,
        val type: RelationType,
        val ref: SEntity,
        val traits: Set<SEntity>,
        override val property: KProperty1<Any, *>,

        val isCollection: Boolean,
        val isOpen: Boolean,
        override val isOptional: Boolean
      ): SFieldOrRelation()

      class SCheck(val name: String, private val check: FieldCheck<Any>) {
        fun check(value: Any): String? {
          return this.check.check(value)
        }
      }

    @Suppress("UNCHECKED_CAST")
    class SListener(val name: String, internal val listener: EListener<*>, internal val enabled: Map<ActionType, Set<EventType>>) {
      internal fun get(action: ActionType, event: EventType): EListener<Any>? {
        return enabled[action]?.let {
          if (it.contains(event)) listener as EListener<Any> else null
        }
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

    val traits = rel.traits.map{ it.name }
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