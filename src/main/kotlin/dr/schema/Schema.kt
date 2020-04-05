package dr.schema

import kotlin.reflect.KClass
import dr.schema.ActionType.*
import dr.schema.EventType.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

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

/* ------------------------- enums -------------------------*/
enum class EntityType {
  MASTER, DETAIL
}

enum class RelationType {
  CREATE, LINK
}

/* ------------------------- structures -------------------------*/
class Schema(
  val masters: Map<String, SEntity>,
  val entities: Map<String, SEntity>,
  val traits: Map<String, STrait>
)

  /* ------------------------- entity -------------------------*/
  class SEntity(val name: String, val type: EntityType, val listeners: List<SListener>): EListener<Any>() {
    lateinit var fields: Map<String, SField>
      internal set

    lateinit var rels: Map<String, SRelation>
      internal set

    override fun onRead(id: Long, tree: Map<String, Any>) {
      listeners.forEach { it.listener.onRead(id, tree) }
    }

    override fun onCreate(type: EventType, id: Long, new: Any) {
      listeners.forEach { it.get(CREATE, type)?.onCreate(type, id, new) }
    }

    override fun onUpdate(type: EventType, id: Long, tree: Map<String, Any>) {
      listeners.forEach { it.get(UPDATE, type)?.onUpdate(type, id, tree) }
    }

    override fun onDelete(type: EventType, id: Long) {
      listeners.forEach { it.get(DELETE, type)?.onDelete(type, id) }
    }

    override fun onAddCreate(type: EventType, id: Long, field: String, new: Any) {
      listeners.forEach { it.get(ADD_CREATE, type)?.onAddCreate(type, id, field, new) }
    }

    override fun onAddLink(type: EventType, id: Long, field: String, link: Long) {
      listeners.forEach { it.get(ADD_LINK, type)?.onAddLink(type, id, field, link) }
    }

    override fun onRemoveLink(type: EventType, id: Long, field: String, link: Long) {
      listeners.forEach { it.get(REMOVE_LINK, type)?.onRemoveLink(type, id, field, link) }
    }
  }

    class SField(
      val type: FieldType,
      val isOptional: Boolean
    )

    class SRelation(
      val type: RelationType,
      val ref: SEntity,
      val traits: Set<STrait>,

      val isCollection: Boolean,
      val isOpen: Boolean,
      val isOptional: Boolean
    )

    @Suppress("UNCHECKED_CAST")
    class SListener(val listener: EListener<*>, val enabled: Map<ActionType, Set<EventType>>) {
      internal fun get(action: ActionType, event: EventType): EListener<Any>? {
        return enabled[action]?.let {
          if (it.contains(event)) listener as EListener<Any> else null
        }
      }
    }

  /* ------------------------- trait -------------------------*/
  class STrait(val name: String, val listeners: List<SListener>) {
    lateinit var fields: Map<String, SField>
      internal set

    lateinit var refs: Map<String, SRelation>
      internal set
  }

/* ----------- Helper printer functions ----------- */
fun Schema.print(filter: String = "all") {
  println("-------SCHEMA (filter=$filter)-------")
  
  val mFiltered = if (filter == "all") this.masters else this.masters.filter{ (key, _) -> key.startsWith(filter) }
  val mTraits = if (filter == "all") this.traits else this.traits.filter{ (key, _) -> key.startsWith(filter) }

  println("<MASTER>")
  for ((name, entity) in mFiltered) {
    println("  $name")
    entity.print(4)
  }

  println("<TRAIT>")
  for ((name, trait) in mTraits) {
    println("  $name")
    trait.print(4)
  }
}

fun SEntity.print(spaces: Int) {
  val tab = " ".repeat(spaces)

  this.fields.print(tab)
  this.rels.print(tab, spaces)

  if (this.listeners.isNotEmpty()) {
    println("${tab}(listeners)")
    this.listeners.print(spaces + 2)
  }
}

fun STrait.print(spaces: Int) {
  val tab = " ".repeat(spaces)

  this.fields.print(tab)

  for ((name, ref) in this.refs) {
    val opt = if (ref.isOptional) "OPT " else ""
    println("${tab}$name: ${ref.ref.name} - ${opt}REF")
    if (ref.ref.type != EntityType.MASTER) {
      ref.ref.print(spaces + 2)
    }
  }

  if (this.listeners.isNotEmpty()) {
    println("${tab}(listeners)")
    this.listeners.print(spaces + 2)
  }
}

fun Map<String, SField>.print(tab: String) {
  for ((name, field) in this) {
    val opt = if (field.isOptional) "OPT " else ""
    println("${tab}$name: ${field.type} - ${opt}FIELD")
  }
}

fun Map<String, SRelation>.print(tab: String, spaces: Int) {
  for ((name, rel) in this) {
    val isOpen = if (rel.isOpen) "OPEN " else ""
    val isOptional = if (rel.isOptional) "OPT " else ""

    val traits = rel.traits.map{ it.name }
    val sTraits = if (traits.isEmpty()) "" else " $traits"
    println("${tab}$name: ${rel.ref.name} - ${isOpen}${isOptional}${rel.type}${sTraits}")
    if (rel.ref.type != EntityType.MASTER) {
      rel.ref.print(spaces + 2)
    }
  }
}

fun List<SListener>.print(spaces: Int) {
  val tab = " ".repeat(spaces)
  for (lis in this) {
    val name = lis.listener.javaClass.kotlin.qualifiedName
    println("${tab}$name -> ${lis.enabled}")
  }
}