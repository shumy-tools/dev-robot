package dr.schema

import kotlin.reflect.*
import kotlin.reflect.full.*
import java.time.*

/* ------------------------- api -------------------------*/
class SParser {
  companion object {
    fun parse(vararg items: KClass<out Any>): Schema {
      println("---Processing Schema---")
      val tmpSchema = TempSchema()
      for (kc in items) {
        if (kc.getEntityType() != EntityType.MASTER) {
          throw Exception("Include only master entities!")
        }

        kc.processEntity(tmpSchema)
      }

      return Schema(tmpSchema.masters, tmpSchema.entities, tmpSchema.traits)
    }
  }
}

/* ------------------------- helpers -------------------------*/
private val TEXT = typeOf<String?>()
private val INT = typeOf<Int?>()
private val LONG = typeOf<Long?>()
private val FLOAT = typeOf<Float?>()
private val DOUBLE = typeOf<Double?>()
private val BOOL = typeOf<Boolean?>()

private val TIME = typeOf<LocalTime?>()
private val DATE = typeOf<LocalDate?>()
private val DATETIME = typeOf<LocalDateTime?>()

private val MAP = typeOf<Map<*, *>?>()
private val LIST = typeOf<List<*>?>()
private val SET = typeOf<Set<*>?>()

private class TempSchema(
  val masters: MutableMap<String, SEntity> = LinkedHashMap(),
  val entities: MutableMap<String, SEntity> = LinkedHashMap(),
  val traits: MutableMap<String, STrait> = LinkedHashMap()
)

private fun KClass<*>.checkEntityNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Master>())
    aNumber++
  
  if (this.hasAnnotation<Detail>())
    aNumber++
  
  if (this.hasAnnotation<Link>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KClass with multiple types! Select one of (Master, Detail, Link). - ($name)")
}

private fun KProperty1<*, *>.checkRelationNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Create>())
    aNumber++
  
  if (this.hasAnnotation<Link>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KProperty with multiple associations! Select one of (Composition, Aggregation). - ($name, ${this.name})")
}

private fun KClass<*>.processEntity(tmpSchema: TempSchema): SEntity {
  val name = this.qualifiedName ?: throw Exception("No Entity name!")
  return tmpSchema.entities.getOrElse(name) {
    this.checkEntityNumber(name)

    println("  Entity: $name")
    val fields = LinkedHashMap<String, SField>()
    val rels = LinkedHashMap<String, SRelation>()

    for (field in this.memberProperties) {
      val rType = field.returnType
      if (rType.isSubtypeOf(MAP))
        throw Exception("Map is not supported in associations! - ($name, ${field.name})")

      if (field.hasAnnotation<Create>() || field.hasAnnotation<Link>()) {
        // SRelation
        val rel = field.processRelation(name, tmpSchema)
        rels[field.name] = rel
      } else {
        // SField
        val type = rType.getFieldType() ?: throw Exception("Unrecognized field type! - ($name, ${field.name})")
        val isOptional = rType.isMarkedNullable
        fields[field.name] = SField(type, isOptional)
      }
    }

    val type = this.getEntityType() ?: throw Exception("Required annotation (Master, Detail)! - ($name)")
    val entity = SEntity(name, type, fields, rels)

    tmpSchema.entities[name] = entity
    if (entity.type == EntityType.MASTER) {
      tmpSchema.masters[name] = entity
    }

    entity
  }
}

private fun KClass<*>.processTrait(tmpSchema: TempSchema): STrait {
  val name = this.qualifiedName ?: throw Exception("No Trait name!")
  return tmpSchema.traits.getOrElse(name) {
    this.checkEntityNumber(name)

    if (!this.hasAnnotation<Trait>())
      throw Exception("Required annotation (Trait)! - ($name)")

    println("    Trait: $name")
    val fields = LinkedHashMap<String, SField>()
    val refs = LinkedHashMap<String, SRelation>()

    for (field in this.memberProperties) {
      val rType = field.returnType
      val type = rType.getFieldType()
      if (type != null) {
        // SField
        val isOptional = rType.isMarkedNullable
        fields[field.name] = SField(type, isOptional)
      } else {
        // SRelation
        val rel = field.processRelation(name, tmpSchema)
        if (!rel.isCollection) {
          refs[field.name] = rel
        } else {
          throw Exception("Traits do not support collections! - ($name, ${field.name})")
        }
      }
    }

    val trait = STrait(name, fields, refs)
    tmpSchema.traits[name] = trait

    trait
  }
}

private fun KProperty1<*, *>.processRelation(name: String, tmpSchema: TempSchema): SRelation {
  this.checkRelationNumber(name)

  val type = this.returnType
  val isOpen = this.hasAnnotation<Open>()
  val isOptional = type.isMarkedNullable

  val traits = LinkedHashSet<STrait>()
  val link = this.findAnnotation<Link>()

  val rType: RelationType = when {
    this.hasAnnotation<Create>() -> RelationType.CREATE
    link != null -> {
      for (trait in link.traits)
        traits.add(trait.processTrait(tmpSchema))
      RelationType.LINK
    }
    else -> throw Exception("Required annotation (Create, Link)! - ($name, ${this.name})")
  }

  val (ref, isCollection) = if (type.isSubtypeOf(LIST) || type.isSubtypeOf(SET)) {
    // collection
    if (isOptional)
      throw Exception("Collections are non optional! - ($name, ${this.name})")

    val tRef = type.arguments.first().type ?: throw Exception("Required generic type! - ($name, ${this.name})")
    val ref = when(rType) {
      RelationType.CREATE -> tRef.processCreate(tmpSchema)
      RelationType.LINK -> link!!.processLink(tRef, name, this.name, tmpSchema)
    }

    Pair(ref, true)
  } else {
    // reference
    val ref = when(rType) {
      RelationType.CREATE -> type.processCreate(tmpSchema)
      RelationType.LINK -> link!!.processLink(type, name, this.name, tmpSchema)
    }

    Pair(ref, false)
  }

  if (rType == RelationType.CREATE && ref.type == EntityType.MASTER) {
    throw Exception("Cannot create a master through a relation! - ($name, ${this.name})")
  }

  return SRelation(rType, ref, traits, isCollection, isOpen, isOptional)
}

private fun KType.processCreate(tmpSchema: TempSchema): SEntity {
  val entity = this.classifier as KClass<*>
  return entity.processEntity(tmpSchema)
}

private fun Link.processLink(tRef: KType, name: String, field: String, tmpSchema: TempSchema): SEntity {
  if (!tRef.isSubtypeOf(LONG))
    throw Exception("Links should have a reference to id (Long)! - ($name, $field)")

  return this.value.processEntity(tmpSchema)
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  return null
}

private fun KType.getFieldType(): FieldType? {
  if (this.isSubtypeOf(TEXT)) return FieldType.TEXT
  if (this.isSubtypeOf(INT) || this.isSubtypeOf(LONG)) return FieldType.INT
  if (this.isSubtypeOf(FLOAT) || this.isSubtypeOf(DOUBLE)) return FieldType.FLOAT
  if (this.isSubtypeOf(BOOL)) return FieldType.BOOL

  if (this.isSubtypeOf(TIME)) return FieldType.TIME
  if (this.isSubtypeOf(DATE)) return FieldType.DATE
  if (this.isSubtypeOf(DATETIME)) return FieldType.DATETIME
  
  return null
}