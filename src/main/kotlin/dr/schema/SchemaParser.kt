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
  val links: MutableMap<String, SLink> = LinkedHashMap<String, SLink>()
)

class SchemaParser(val schema: Schema) {
  companion object {
    fun parse(vararg items: KClass<out Any>): Schema {
      println("---Processing Schema---")
      val tmpSchema = TempSchema()
      for (kc in items) {
        if (kc.getEntityType() != EntityType.MASTER) {
          throw Exception("Include only master entities!")
        }

        val entity = kc.processEntity(tmpSchema)
        tmpSchema.masters.put(entity.name, entity)
      }

      return Schema(tmpSchema.masters, tmpSchema.entities, tmpSchema.links)
    }
  }
}



private fun KClass<*>.checkTypeNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Master>())
    aNumber++
  
  if (this.hasAnnotation<Item>())
    aNumber++
  
  if (this.hasAnnotation<Link>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KClass with multiple types! Select one of (Master, Item, Link). - ($name)")
}

private fun KProperty1<*, *>.checkAssociationNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Composition>())
    aNumber++
  
  if (this.hasAnnotation<Aggregation>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KProperty with multiple associations! Select one of (Composition, Aggregation). - ($name, ${this.name})")
}

private fun KClass<*>.processEntity(tmpSchema: TempSchema): SEntity {
  val exist = tmpSchema.entities.get(this.qualifiedName)
  if (exist != null) {
    return exist
  }

  val name = this.qualifiedName ?: throw Exception("No Entity name!")
  this.checkTypeNumber(name)

  println("  KClass: $name")
  val fields = LinkedHashMap<String, SField>()
  val refs = LinkedHashMap<String, SReference>()
  val assos = LinkedHashMap<String, SAssociation>()
  
  for (field in this.memberProperties) {
    val rType = field.returnType
    
    if (rType.isSubtypeOf(MAP))
      throw Exception("Map is not supported in associations! - ($name, ${field.name})")

    val isOptional = rType.isMarkedNullable
    val type = rType.getFieldType()

    if (type != null) {
      // SField
      fields.put(field.name, SField(type, isOptional))
    } else if (rType.isSubtypeOf(LIST) || rType.isSubtypeOf(SET)) {
      // SAssociation
      field.checkAssociationNumber(name)

      if (isOptional)
        throw Exception("Associations are non optional! - ($name, ${field.name})")

      val isOpen = field.hasAnnotation<Open>()
      val (aType, links) = field.processAssociation(tmpSchema) ?: throw Exception("Required annotation (Composition, Aggregation)! - ($name, ${field.name})")

      val tRef = rType.arguments.first().type ?: throw Exception("Required generic type! - ($name, ${field.name})")
      val ref = tRef.processReference(tmpSchema)
      
      assos.put(field.name, SAssociation(aType, ref, links, isOpen))
    } else {
      // SReference
      val ref = rType.processReference(tmpSchema)
      refs.put(field.name, SReference(ref, isOptional))
    }
  }
  
  val type = this.getEntityType() ?: throw Exception("Required annotation (Master, Item)! - ($name)")
  val entity = SEntity(name, type, fields, refs, assos)
  tmpSchema.entities.put(name, entity)

  return entity
}

private fun KType.processReference(tmpSchema: TempSchema): SEntity {
  return tmpSchema.entities.getOrElse(this.toString()) {
    val clazz = this.classifier as KClass<*>
    clazz.processEntity(tmpSchema)
  }
}

private fun KProperty1<*, *>.processAssociation(tmpSchema: TempSchema): Pair<AssociationType, Set<SLink>>? {
  val comp = this.findAnnotation<Composition>()
  if (comp != null) {
    val links = comp.value.map{ it.processLink(tmpSchema) }.toSet()
    return Pair(AssociationType.COMPOSITION, links)
  }

  val aggr = this.findAnnotation<Aggregation>()
  if (aggr != null) {
    val links = aggr.value.map{ it.processLink(tmpSchema) }.toSet()
    return Pair(AssociationType.AGGREGATION, links)
  }

  return null
}

private fun KClass<*>.processLink(tmpSchema: TempSchema): SLink {
  val name = this.qualifiedName ?: throw Exception("No Link name!")
  this.checkTypeNumber(name)

  if (!this.hasAnnotation<Link>())
    throw Exception("Not of type Link! - ($name)")
  
  return tmpSchema.links.getOrElse(name) {
    val fields = LinkedHashMap<String, SField>()
    val refs = LinkedHashMap<String, SReference>()
    for (field in this.memberProperties) {
      val rType = field.returnType
      val isOptional = rType.isMarkedNullable
      val type = rType.getFieldType()

      if (type != null) {
        // SField
        fields.put(field.name, SField(type, isOptional))
      } else {
        // SReference
        val ref = rType.processReference(tmpSchema)
        refs.put(field.name, SReference(ref, isOptional))
      }
    }

    val link = SLink(name, fields, refs)
    tmpSchema.links.put(name, link)
    link
  }
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Item>()) return EntityType.ITEM
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