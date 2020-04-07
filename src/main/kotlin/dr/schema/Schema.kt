package dr.schema

import dr.schema.ActionType.*
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

/* ------------------------- enums -------------------------*/
enum class EntityType {
  MASTER, DETAIL, TRAIT
}

enum class RelationType {
  CREATE, LINK
}

/* ------------------------- structures -------------------------*/
class Traits(vararg val traits: Any) /*{
  override fun toString(): String {
    return "[${traits.forEach { it.javaClass.kotlin.qualifiedName }}]"
  }
}*/

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

    override fun onCreate(type: EventType, id: Long?, new: Any) {
      listeners.forEach { it.get(CREATE, type)?.onCreate(type, id, new) }
    }

    override fun onUpdate(type: EventType, id: Long, data: Map<String, Any?>) {
      listeners.forEach { it.get(UPDATE, type)?.onUpdate(type, id, data) }
    }

    override fun onDelete(type: EventType, id: Long) {
      listeners.forEach { it.get(DELETE, type)?.onDelete(type, id) }
    }

    override fun onAdd(type: EventType, id: Long?, sRelation: SRelation, link: Long?, new: Any) {
      listeners.forEach { it.get(ADD_CREATE, type)?.onAdd(type, id, sRelation, link, new) }
    }

    override fun onLink(type: EventType, id: Long?, sRelation: SRelation, link: Long) {
      listeners.forEach { it.get(ADD_LINK, type)?.onLink(type, id, sRelation, link) }
    }

    override fun onRemove(type: EventType, id: Long, sRelation: SRelation, link: Long) {
      listeners.forEach { it.get(REMOVE_LINK, type)?.onRemove(type, id, sRelation, link) }
    }
  }

    class SField(
      val name: String,
      val type: FieldType,
      val checks: Set<SCheck>,
      private val property: KProperty1<Any, *>,

      val isOptional: Boolean
    ) {
      var isInput: Boolean = false
        internal set

      fun getValue(instance: Any): Any? {
        return if (instance is Map<*, *>) {
          instance[name]
        } else {
          this.property.get(instance)
        }
      }
    }

    class SRelation(
      val name: String,
      val type: RelationType,
      val ref: SEntity,
      val traits: Set<SEntity>,
      private val property: KProperty1<Any, *>,

      val isCollection: Boolean,
      val isOpen: Boolean,
      val isOptional: Boolean
    ) {
      var isInput: Boolean = false
        internal set

      fun getValue(instance: Any): Any? {
        return if (instance is Map<*, *>) {
          instance[name]
        } else {
          this.property.get(instance)
        }
      }
    }

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