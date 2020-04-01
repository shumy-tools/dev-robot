package dr.schema

enum class EntityType {
  MASTER, ITEM
}

enum class FieldType {
  TEXT, INTEGER, NUMBER,
  TIME, DATE, DATETIME
}

enum class AssociationType {
  COMPOSITION, AGGREGATION
}

class Schema(
  val masters: Map<String, SEntity>,
  val entities: Map<String, SEntity>,
  val links: Map<String, SLink>
) {
  fun print(filter: String = "all") {
    println("-------SCHEMA (filter=$filter)-------")
    
    val mFiltered = if (filter == "all") this.masters else this.masters.filter{ (key, _) -> key.startsWith(filter) }
    val mLinks = if (filter == "all") this.links else this.links.filter{ (key, _) -> key.startsWith(filter) }

    println("<MASTER>")
    for ((name, entity) in mFiltered) {
      println("  $name")
      entity.print(4)
    }

    println("<LINK>")
    for ((name, link) in mLinks) {
      println("  $name")
      link.print(4)
    }
  }
}

class SEntity(
  val name: String,
  val type: EntityType,
  val fields: Map<String, SField>,
  val refs: Map<String, SReference>,
  val assos: Map<String, SAssociation>
) {
  fun print(spaces: Int) {
    val item = " ".repeat(spaces)

    for ((name, field) in this.fields) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.type} - ${opt}FIELD")
    }

    for ((name, field) in this.refs) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.ref.name} - ${opt}REF")
      if (field.ref.type != EntityType.MASTER) {
        field.ref.print(spaces + 2)
      }
    }

    for ((name, field) in this.assos) {
      val links = field.links.map{ it.name }
      val open = if (field.isOpen) "OPEN " else ""
      val sLinks = if (links.isEmpty()) "" else " $links"
      println("${item}$name: ${field.ref.name} - ${open}${field.type}${sLinks}")
      if (field.ref.type != EntityType.MASTER) {
        field.ref.print(spaces + 2)
      }
    }
  }
}

class SLink(
  val name: String,
  val fields: Map<String, SField>,
  val refs: Map<String, SReference>
) {
  fun print(spaces: Int) {
    val item = " ".repeat(spaces)

    for ((name, field) in this.fields) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.type} - ${opt}FIELD")
    }

    for ((name, field) in this.refs) {
      val opt = if (field.isOptional) "OPT " else ""
      println("${item}$name: ${field.ref.name} - ${opt}REF")
      if (field.ref.type != EntityType.MASTER) {
        field.ref.print(spaces + 2)
      }
    }
  }
}

class SField(
  val type: FieldType,
  val isOptional: Boolean
)

class SReference(
  val ref: SEntity,
  val isOptional: Boolean
)

class SAssociation(
  val type: AssociationType,
  val ref: SEntity,
  val links: Set<SLink>,
  val isOpen: Boolean
)