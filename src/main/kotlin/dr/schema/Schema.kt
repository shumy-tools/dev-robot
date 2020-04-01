package dr.schema

enum class EntityType {
  MASTER, DETAIL
}

enum class FieldType {
  TEXT, INTEGER, NUMBER,
  TIME, DATE, DATETIME
}

enum class RelationType {
  CREATE, LINK
}

class Schema(
  val masters: Map<String, SEntity>,
  val entities: Map<String, SEntity>,
  val traits: Map<String, STrait>
) {
  fun print(filter: String = "all") {
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
}

class SEntity(
  val name: String,
  val type: EntityType,
  val fields: Map<String, SField>,
  val refs: Map<String, SRelation>,
  val rels: Map<String, SRelation>
) {
  fun print(spaces: Int) {
    val item = " ".repeat(spaces)

    for ((name, field) in this.fields) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.type} - ${opt}FIELD")
    }

    for ((name, ref) in this.refs) {
      val opt = if (ref.isOptional) "OPT " else ""
      println("${item}$name: ${ref.ref.name} - ${opt}REF")
      if (ref.ref.type != EntityType.MASTER) {
        ref.ref.print(spaces + 2)
      }
    }

    for ((name, rel) in this.rels) {
      val open = if (rel.isOpen) "OPEN " else ""
      val traits = rel.traits.map{ it.name }
      val sTraits = if (traits.isEmpty()) "" else " $traits"
      println("${item}$name: ${rel.ref.name} - ${open}${rel.type}${sTraits}")
      if (rel.ref.type != EntityType.MASTER) {
        rel.ref.print(spaces + 2)
      }
    }
  }
}

class STrait(
  val name: String,
  val fields: Map<String, SField>,
  val refs: Map<String, SRelation>
) {
  fun print(spaces: Int) {
    val item = " ".repeat(spaces)

    for ((name, field) in this.fields) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.type} - ${opt}FIELD")
    }

    for ((name, ref) in this.refs) {
      val opt = if (ref.isOptional) "OPT " else ""
      println("${item}$name: ${ref.ref.name} - ${opt}REF")
      if (ref.ref.type != EntityType.MASTER) {
        ref.ref.print(spaces + 2)
      }
    }
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