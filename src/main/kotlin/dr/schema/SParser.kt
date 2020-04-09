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

        kc.processEntity(true, tmpSchema)
      }

      return Schema(tmpSchema.masters, tmpSchema.entities, tmpSchema.traits)
    }
  }
}

/* ------------------------- helpers -------------------------*/
private class TempSchema(
  val tmpOwned: MutableMap<String, SEntity> = LinkedHashMap(),
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
    throw Exception("KClass with multiple entity types! Select one of (Master, Detail, Trait). - ($name)")
}

private fun KProperty1<*, *>.checkRelationNumber(name: String) {
  var aNumber = 0
  if (this.hasAnnotation<Create>())
    aNumber++
  
  if (this.hasAnnotation<Link>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KProperty with multiple relation types! Select one of (Create, Link). - ($name, ${this.name})")
}

private fun KClass<*>.processEntity(owned: Boolean, tmpSchema: TempSchema): SEntity {
  val name = this.qualifiedName ?: throw Exception("No Entity name!")
  if (owned && tmpSchema.tmpOwned[name] != null)
    throw Exception("Entity already owned! - ($name)")

  return tmpSchema.entities.getOrElse(name) {
    this.checkEntityNumber(name)

    println("  Entity: $name")
    if(!this.isData)
      throw Exception("Entities must be data classes! - ($name)")

    val type = this.getEntityType() ?: throw Exception("Required annotation, one of (Master, Detail, Trait)! - ($name)")
    val entity = SEntity(name, type, processListeners())

    if (owned)
      tmpSchema.tmpOwned[name] = entity

    tmpSchema.entities[name] = entity
    if (entity.type == EntityType.TRAIT) {
      tmpSchema.traits[name] = entity
    } else if (entity.type == EntityType.MASTER) {
      tmpSchema.masters[name] = entity
    }

    // process fields and relations
    val tmpInputProps = this.primaryConstructor!!.parameters.map { it.name }.toSet()
    val tmpFields = LinkedHashMap<String, SField>()
    val tmpRels = LinkedHashMap<String, SRelation>()

    for (prop in memberProperties) {
      if (prop.name.startsWith("ref") || prop.name.startsWith("inv"))
        throw Exception("Reserved names starting with 'ref' or 'inv'! - ($name, ${prop.name})")

      val fieldOrRelation = prop.processFieldOrRelation(name, tmpSchema).apply { isInput = tmpInputProps.contains(prop.name) }
      if (fieldOrRelation is SField) {
        tmpFields[prop.name] = fieldOrRelation
      } else {
        tmpRels[prop.name] = (fieldOrRelation as SRelation)
      }
    }

    entity.apply {
      fields = tmpFields
      rels = tmpRels
    }
  }
}

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
    @Suppress("UNCHECKED_CAST")
    val instance = it.createInstance() as FieldCheck<Any>
    SCheck(it.qualifiedName!!, instance)
  }.toSet()
}

private fun KClass<*>.processListeners(): Set<SListener> {
  val listeners = findAnnotation<Listeners>() ?: return setOf()
  return listeners.value.map {
    val sType = it.supertypes.firstOrNull {
      sType -> EListener::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
    } ?: throw Exception("Listener '${it.qualifiedName}' must inherit from '${EListener::class.qualifiedName}'")

    val tRef = sType.arguments.first().type ?: throw Exception("Listener '${it.qualifiedName}' requires generic type!")
    if (tRef != this.createType())
      throw throw Exception("Listener '${it.qualifiedName}' must inherit from '${EListener::class.qualifiedName}<${this.qualifiedName}>'")

    // instantiate listener
    val instance = it.createInstance() as EListener<*>
    val enabled = it.declaredFunctions.mapNotNull { member ->
      val action = when(member.name) {
        ActionType.CREATE.funName -> ActionType.CREATE
        ActionType.UPDATE.funName -> ActionType.UPDATE
        ActionType.DELETE.funName -> ActionType.DELETE
        ActionType.ADD.funName -> ActionType.ADD
        ActionType.LINK.funName -> ActionType.LINK
        ActionType.UNLINK.funName -> ActionType.UNLINK
        else -> null
      }

      if (action != null) {
        val events = member.findAnnotation<Events>() ?: throw Exception("'${member.name}' on '${it.qualifiedName}' listener requires 'Events' annotation!")
        if (events.value.isEmpty())
          throw Exception("${member.name} on '${it.qualifiedName}' listener requires at least one EventType!")

        action to events.value.map { evt -> evt }.toSet()
      } else null
    }.toMap()

    SListener(it.qualifiedName!!, instance, enabled)
  }.toSet()
}

private fun KProperty1<*, *>.processFieldOrRelation(name: String, tmpSchema: TempSchema): SFieldOrRelation {
  val rType = this.returnType
  if (this is KMutableProperty1<*, *>)
    throw Exception("All fields must be immutable! - ($name, ${this.name})")

  return if (this.hasAnnotation<Create>() || this.hasAnnotation<Link>()) {
    // SRelation
    this.processRelation(name, tmpSchema)
  } else {
    // SField
    val type = TypeEngine.convert(rType) ?: throw Exception("Unrecognized field type! - ($name, ${this.name})")
    val checks = this.processChecks()

    @Suppress("UNCHECKED_CAST")
    SField(this.name, type, checks, this as KProperty1<Any, *>, rType.isMarkedNullable)
  }
}

private fun KProperty1<*, *>.processRelation(name: String, tmpSchema: TempSchema): SRelation {
  this.checkRelationNumber(name)

  val type = this.returnType
  val isOpen = this.hasAnnotation<Open>()
  val isOptional = type.isMarkedNullable

  val traits = LinkedHashSet<SEntity>()
  val link = this.findAnnotation<Link>()

  val rType = when {
    this.hasAnnotation<Create>() -> RelationType.CREATE
    link != null -> {
      for (trait in link.traits)
        traits.add(trait.processEntity(false, tmpSchema))
      RelationType.LINK
    }
    else -> throw Exception("Required annotation, one of (Create, Link)! - ($name, ${this.name})")
  }

  val (ref, isCollection) = if (type.isSubtypeOf(TypeEngine.LIST) || type.isSubtypeOf(TypeEngine.SET) || type.isSubtypeOf(TypeEngine.MAP_ID_TRAITS)) {
    if (isOptional)
      throw Exception("Collections cannot be optional! - ($name, ${this.name})")

    val ref = when(rType) {
      RelationType.CREATE -> {
        if (!type.isSubtypeOf(TypeEngine.LIST) && !type.isSubtypeOf(TypeEngine.SET))
          throw Exception("Create-collection must be of type, one of (Set<*>, List<*>)! - ($name, ${this.name})")

        val tRef = type.arguments.last().type ?: throw Exception("Required generic type! - ($name, ${this.name})")
        tRef.processCreate(tmpSchema)
      }

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.LIST_ID) && !type.isSubtypeOf(TypeEngine.SET_ID))
          throw Exception("Link-collection without traits must be of type, one of (List<Long>, Set<Long>)! - ($name, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.MAP_ID_TRAITS))
          throw Exception("Link-collection with traits type must be of type Map<Long, Traits>! - ($name, ${this.name})")

        link!!.value.processEntity(false, tmpSchema)
      }
    }

    Pair(ref, true)
  } else {
    val ref = when(rType) {
      RelationType.CREATE -> type.processCreate(tmpSchema)

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.ID))
          throw Exception("Link-reference without traits must be of type Long! - ($name, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.PAIR_ID_TRAITS))
          throw Exception("Link-reference with traits must be of type Pair<Long, Traits>! - ($name, ${this.name})")

        link!!.value.processEntity(false, tmpSchema)
      }
    }

    Pair(ref, false)
  }

  if (rType == RelationType.CREATE && ref.type == EntityType.MASTER)
    throw Exception("Cannot create a master through a relation! - ($name, ${this.name})")

  @Suppress("UNCHECKED_CAST")
  return SRelation(this.name, rType, ref, traits, this as KProperty1<Any, *>, isCollection, isOpen, isOptional)
}

private fun KType.processCreate(tmpSchema: TempSchema): SEntity {
  val entity = this.classifier as KClass<*>
  return entity.processEntity(true, tmpSchema)
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  if (this.hasAnnotation<Trait>()) return EntityType.TRAIT
  return null
}