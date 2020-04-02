package dr.schema

import kotlin.reflect.*
import kotlin.reflect.full.*
import java.time.*

val STRING = typeOf<String?>()
val INT = typeOf<Int?>()
val LONG = typeOf<Long?>()
val FLOAT = typeOf<Float?>()
val DOUBLE = typeOf<Double?>()

val TIME = typeOf<LocalTime?>()
val DATE = typeOf<LocalDate?>()
val DATETIME = typeOf<LocalDateTime?>()

val MAP = typeOf<Map<*, *>?>()
val LIST = typeOf<List<*>?>()
val SET = typeOf<Set<*>?>()

class TempSchema(
  val masters: MutableMap<String, SEntity> = LinkedHashMap<String, SEntity>(),
  val entities: MutableMap<String, SEntity> = LinkedHashMap<String, SEntity>(),
  val traits: MutableMap<String, STrait> = LinkedHashMap<String, STrait>()
)

class SchemaParser() {
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


/* ----------- Helper processing functions ----------- */
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
    val refs = LinkedHashMap<String, SRelation>()
    val rels = LinkedHashMap<String, SRelation>()

    for (field in this.memberProperties) {
      val rType = field.returnType
      if (rType.isSubtypeOf(MAP))
        throw Exception("Map is not supported in associations! - ($name, ${field.name})")
      
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
          rels[field.name] = rel
        }
      }
    }

    val type = this.getEntityType() ?: throw Exception("Required annotation (Master, Detail)! - ($name)")
    val entity = SEntity(name, type, fields, refs, rels)

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
        // SReference
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

  var rType: RelationType? = null
  val traits = LinkedHashSet<STrait>()

  if (this.hasAnnotation<Create>()) {
    rType = RelationType.CREATE
  }

  val link = this.findAnnotation<Link>()
  if (link != null) {
    rType = RelationType.LINK
    for (trait in link.value) {
      traits.add(trait.processTrait(tmpSchema))
    }
  }

  if (rType == null) 
    throw Exception("Required annotation (Create, Link)! - ($name, ${this.name})")

  val (ref, isCollection) = if (type.isSubtypeOf(LIST) || type.isSubtypeOf(SET)) {
    // SCollection
    if (isOptional)
      throw Exception("Collections are non optional! - ($name, ${this.name})")

    val tRef = type.arguments.first().type ?: throw Exception("Required generic type! - ($name, ${this.name})")
    val ref = tRef.processReference(tmpSchema)
    Pair(ref, true)
  } else {
    // SReference
    val ref = type.processReference(tmpSchema)
    Pair(ref, false)
  }

  if (rType == RelationType.CREATE && ref.type == EntityType.MASTER) {
    throw Exception("Cannot create a master through a relation! - ($name, ${this.name})")
  }

  return SRelation(rType, ref, traits, isCollection, isOpen, isOptional)
}

private fun KType.processReference(tmpSchema: TempSchema): SEntity {
  val entity = this.classifier as KClass<*>
  return entity.processEntity(tmpSchema)
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  return null
}

private fun KType.getFieldType(): FieldType? {
  if (this.isSubtypeOf(STRING)) return FieldType.TEXT
  if (this.isSubtypeOf(INT) || this.isSubtypeOf(LONG)) return FieldType.INTEGER
  if (this.isSubtypeOf(FLOAT) || this.isSubtypeOf(DOUBLE)) return FieldType.NUMBER

  if (this.isSubtypeOf(TIME)) return FieldType.TIME
  if (this.isSubtypeOf(DATE)) return FieldType.DATE
  if (this.isSubtypeOf(DATETIME)) return FieldType.DATETIME
  
  return null
}