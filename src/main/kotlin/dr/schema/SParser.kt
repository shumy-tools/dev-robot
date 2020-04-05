package dr.schema

import kotlin.reflect.*
import kotlin.reflect.full.*

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
private val MAP = typeOf<Map<*, *>?>()
private val LIST = typeOf<List<*>?>()
private val SET = typeOf<Set<*>?>()

private class TempSchema(
  val masters: MutableMap<String, SEntity> = LinkedHashMap(),
  val entities: MutableMap<String, SEntity> = LinkedHashMap(),
  val traits: MutableMap<String, SEntity> = LinkedHashMap()
)

private fun KClass<*>.checkEntityNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Master>())
    aNumber++
  
  if (this.hasAnnotation<Detail>())
    aNumber++
  
  if (this.hasAnnotation<Trait>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KClass with multiple types! Select one of (Master, Detail, Trait). - ($name)")
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
    if(!this.isData)
      throw Exception("Entities must be data classes! - ($name)")

    val type = this.getEntityType() ?: throw Exception("Required annotation (Master, Detail, Trait)! - ($name)")
    val entity = SEntity(name, type, processListeners())

    tmpSchema.entities[name] = entity
    if (entity.type == EntityType.TRAIT) {
      tmpSchema.traits[name] = entity
    } else if (entity.type == EntityType.MASTER) {
      tmpSchema.masters[name] = entity
    }

    val tmpAllProps = this.memberProperties.map { it.name to it }.toMap()
    val tmpInputProps = this.primaryConstructor!!.parameters.map { it.name }.toSet()

    val tmpFields = LinkedHashMap<String, SField>()
    val tmpRels = LinkedHashMap<String, SRelation>()

    // process input fields
    for (field in this.primaryConstructor!!.parameters) {
      tmpAllProps[field.name]?.let {
        val fieldOrRelation = it.processFieldOrRelation(name, tmpSchema)
        if (fieldOrRelation is SField) {
          tmpFields[field.name!!] = fieldOrRelation.apply { isInput = true }
        } else {
          tmpRels[field.name!!] = (fieldOrRelation as SRelation).apply { isInput = true }
        }
      }
    }

    // process internal fields
    for (field in this.memberProperties.filter { !tmpInputProps.contains(it.name) }) {
      val fieldOrRelation = field.processFieldOrRelation(name, tmpSchema)
      if (fieldOrRelation is SField) {
        tmpFields[field.name] = fieldOrRelation
      } else {
        tmpRels[field.name] = fieldOrRelation as SRelation
      }
    }

    entity.apply {
      fields = tmpFields
      rels = tmpRels
    }
  }
}

@Suppress("UNCHECKED_CAST")
private fun KProperty1<*, *>.processChecks(): Set<SCheck> {
  val checks = findAnnotation<Checks>() ?: return setOf()
  return checks.value.map {
    val sType = it.supertypes.firstOrNull {
      sType -> FieldCheck::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
    } ?: throw Exception("Check '${it.qualifiedName}' must implement '${FieldCheck::class.qualifiedName}'")

    val tRef = sType.arguments.first().type ?: throw Exception("Check '${it.qualifiedName}' requires generic type!")
    if (tRef != this.returnType)
      throw throw Exception("Check '${it.qualifiedName}: ${FieldCheck::class.qualifiedName}<${(tRef.classifier as KClass<*>).simpleName}>' is not compatible with the field type '${(this.returnType.classifier as KClass<*>).simpleName}'!")

    // instantiate check
    val instance = it.createInstance() as FieldCheck<Any>
    SCheck(it.qualifiedName!!, instance)
  }.toSet()
}

private fun KClass<*>.processListeners(): Set<SListener> {
  val listeners = findAnnotation<Listeners>() ?: return setOf()
  return listeners.value.map {
    val sType = it.supertypes.firstOrNull {
      sType -> EListener::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
    } ?: throw Exception("Listener '${it.qualifiedName}' must inherit '${EListener::class.qualifiedName}'")

    val tRef = sType.arguments.first().type ?: throw Exception("Listener '${it.qualifiedName}' requires generic type!")
    if (tRef != this.createType())
      throw throw Exception("Listener '${it.qualifiedName}' must inherit from '${EListener::class.qualifiedName}<${this.qualifiedName}>'")

    // instantiate listener
    val instance = it.createInstance() as EListener<*>
    val enabled = it.declaredFunctions.mapNotNull { member ->
      val action = when(member.name) {
        "onCreate" -> ActionType.CREATE
        "onUpdate" -> ActionType.UPDATE
        "onDelete" -> ActionType.DELETE
        "onAddCreate" -> ActionType.ADD_CREATE
        "onAddLink" -> ActionType.ADD_LINK
        "onRemoveLink" -> ActionType.REMOVE_LINK
        else -> null
      }

      if (action != null) {
        val events = member.findAnnotation<Events>() ?: throw Exception("'${member.name}' on '${it.qualifiedName}' listener requires 'Events' annotation!")
        if (events.value.isEmpty())
          throw Exception("${member.name} on '${it.qualifiedName}' listener requires at least one EventType!")

        action to events.value.map { evt -> evt }.toSet()
      } else null
    }.toMap()

    SListener(instance, enabled)
  }.toSet()
}

private fun KProperty1<*, *>.processFieldOrRelation(name: String, tmpSchema: TempSchema): Any {
  if (this is KMutableProperty1<*, *>)
    throw Exception("All fields must be immutable! - ($name, ${this.name})")

  val rType = this.returnType
  if (rType.isSubtypeOf(MAP))
    throw Exception("Map is not supported in relations! - ($name, ${this.name})")

  return if (this.hasAnnotation<Create>() || this.hasAnnotation<Link>()) {
    // SRelation
    this.processRelation(name, tmpSchema)
  } else {
    // SField
    val type = TypeEngine.convert(rType) ?: throw Exception("Unrecognized field type! - ($name, ${this.name})")
    val checks = this.processChecks()
    SField(type, checks, rType.isMarkedNullable)
  }
}

private fun KProperty1<*, *>.processRelation(name: String, tmpSchema: TempSchema): SRelation {
  this.checkRelationNumber(name)

  val type = this.returnType
  val isOpen = this.hasAnnotation<Open>()
  val isOptional = type.isMarkedNullable

  val traits = LinkedHashSet<SEntity>()
  val link = this.findAnnotation<Link>()

  val rType: RelationType = when {
    this.hasAnnotation<Create>() -> RelationType.CREATE
    link != null -> {
      for (trait in link.traits)
        traits.add(trait.processEntity(tmpSchema))
      RelationType.LINK
    }
    else -> throw Exception("Required annotation (Create, Link)! - ($name, ${this.name})")
  }

  val (ref, isCollection) = if (type.isSubtypeOf(LIST) || type.isSubtypeOf(SET)) {
    // collection
    if (isOptional)
      throw Exception("Collections cannot be optional! - ($name, ${this.name})")

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

  if (rType == RelationType.CREATE && ref.type == EntityType.MASTER)
    throw Exception("Cannot create a master through a relation! - ($name, ${this.name})")

  return SRelation(rType, ref, traits, isCollection, isOpen, isOptional)
}

private fun KType.processCreate(tmpSchema: TempSchema): SEntity {
  val entity = this.classifier as KClass<*>
  return entity.processEntity(tmpSchema)
}

private fun Link.processLink(tRef: KType, name: String, field: String, tmpSchema: TempSchema): SEntity {
  if (!tRef.isSubtypeOf(TypeEngine.ID))
    throw Exception("Links should have a reference to id (Long)! - ($name, $field)")

  return this.value.processEntity(tmpSchema)
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  if (this.hasAnnotation<Trait>()) return EntityType.TRAIT
  return null
}