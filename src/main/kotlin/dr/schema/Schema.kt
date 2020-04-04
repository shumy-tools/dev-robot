package dr.schema

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
  MASTER, DETAIL
}

enum class FieldType {
  TEXT, INT, FLOAT, BOOL,
  TIME, DATE, DATETIME
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
  class SEntity(
    val name: String,
    val type: EntityType,
    val fields: Map<String, SField>,
    val rels: Map<String, SRelation>,

    val listeners: List<SListener>
  )

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

    class SListener(
      val listener: EListener<*>,
      val enabled: Map<ActionType, Set<EventType>>
    )

  /* ------------------------- trait -------------------------*/
  class STrait(
    val name: String,
    val fields: Map<String, SField>,
    val refs: Map<String, SRelation>,

    val listeners: List<SListener>
  )


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