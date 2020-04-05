package dr.schema

import dr.schema.ActionType.*
import kotlin.reflect.KClass

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
  MASTER, DETAIL, TRAIT
}

enum class RelationType {
  CREATE, LINK
}

/* ------------------------- structures -------------------------*/
class Schema(
  val masters: Map<String, SEntity>,
  val entities: Map<String, SEntity>,
  val traits: Map<String, SEntity>
)

  /* ------------------------- entity -------------------------*/
  class SEntity(val name: String, val type: EntityType, val listeners: Set<SListener>): EListener<Any>() {
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

    override fun onUpdate(type: EventType, id: Long, data: Map<String, Any>) {
      listeners.forEach { it.get(UPDATE, type)?.onUpdate(type, id, data) }
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
      val checks: Set<SCheck>,

      val isOptional: Boolean
    ) {
      var isInput: Boolean = false
        internal set
    }

    class SRelation(
      val type: RelationType,
      val ref: SEntity,
      val traits: Set<SEntity>,

      val isCollection: Boolean,
      val isOpen: Boolean,
      val isOptional: Boolean
    ) {
      var isInput: Boolean = false
        internal set
    }

    class SCheck(val name: String, private val check: FieldCheck<Any>) {
      fun check(value: Any): String? {
        return this.check.check(value)
      }
    }

    @Suppress("UNCHECKED_CAST")
    class SListener(internal val listener: EListener<*>, internal val enabled: Map<ActionType, Set<EventType>>) {
      internal fun get(action: ActionType, event: EventType): EListener<Any>? {
        return enabled[action]?.let {
          if (it.contains(event)) listener as EListener<Any> else null
        }
      }
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
      rel.ref.print(spaces + 2)
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