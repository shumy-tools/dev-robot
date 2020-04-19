package dr.schema

import dr.state.Machine
import dr.state.StateMachine
import kotlin.reflect.*
import kotlin.reflect.full.*

/* ------------------------- api -------------------------*/
object SParser {
  fun parse(vararg items: KClass<out Any>): Schema {
    println("----Checking Master Entities----")
    val tmpSchema = TempSchema()
    for (kc in items) {
      if (kc.getEntityType() != EntityType.MASTER)
        throw Exception("Include only master entities!")

      kc.processEntity(tmpSchema)
      println("    ${kc.qualifiedName} - OK")
    }

    return tmpSchema.schema
  }
}

/* ------------------------- helpers -------------------------*/
private class TempSchema {
  val schema = Schema()
  val owned = mutableMapOf<String, String>()        // ownedEntity -> byEntity
}

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

@Suppress("UNCHECKED_CAST")
private fun KClass<out Any>.processEntity(tmpSchema: TempSchema, ownedBy: String? = null): SEntity {
  val name = this.qualifiedName ?: throw Exception("No Entity name!")
  return tmpSchema.schema.entities.getOrElse(name) {
    this.checkEntityNumber(name)

    if (name.toLowerCase() == "super")
      throw Exception("Reserved class name: 'super'! - ($name)")

    if (this.isOpen || this.isSealed)
      throw Exception("Class inheritance is done via @Extend. Remove open or sealed keywords. - ($name)")

    val type = this.getEntityType() ?: throw Exception("Required annotation, one of (Master, Detail, Trait)! - ($name)")

    // find @LateInit function if exists
    var initFun: KFunction<*>? = null
    for (mFun in declaredMemberFunctions) {
      if (mFun.hasAnnotation<LateInit>()) {
        if (initFun != null)
          throw Exception("Only one @LateInit function is valid! - ($name) - (in ${mFun.name} and ${initFun.name})")

        if(mFun.visibility == KVisibility.PRIVATE)
          throw Exception("@LateInit function cannot be private! - ($name, ${mFun.name})")

        if(mFun.valueParameters.isNotEmpty())
          throw Exception("@LateInit function cannot have parameters! - ($name, ${mFun.name})")

        initFun = mFun
      }
    }

    val sealed = findAnnotation<Sealed>()
    val sEntity = SEntity(this, type, sealed != null, initFun, processListeners())

    // include state machine
    val machine = findAnnotation<StateMachine>()
    if (machine != null) {
      if (type != EntityType.MASTER)
        throw Exception("Only a @Master can have a state machine! - ($name)")

      machine.value.supertypes.firstOrNull {
        sType -> Machine::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
      } ?: throw Exception("StateMachine '${machine.value.qualifiedName}' must implement '${Machine::class.qualifiedName}'")

      machine.value.constructors.firstOrNull {
        it.parameters.isEmpty()
      } ?: throw Exception("StateMachine '${machine.value.qualifiedName}' requires an empty default constructor!")

      sEntity.machine = machine.value
    }

    tmpSchema.schema.addEntity(sEntity)
    if (ownedBy != null)
      tmpSchema.owned[name] = ownedBy

    val tmpInputProps = mutableSetOf<KProperty1<Any, *>>()
    val allProps = memberProperties.map { it.name to (it as KProperty1<Any, *>) }.toMap()

    // process ordered inputs
    for (param in this.primaryConstructor!!.parameters) {
      val prop  = allProps[param.name]!!
      if (prop is KMutableProperty1<*, *>)
        throw Exception("All primary-constructor properties must be immutable! - (${sEntity.name}, ${prop.name})")

      val fieldOrRelation = prop.processFieldOrRelation(sEntity, tmpSchema).apply { isInput = true }
      sEntity.addProperty(prop.name, fieldOrRelation)
      tmpInputProps.add(prop)
    }

    // process derived properties and 'var' inputs
    for (prop in allProps.values.filter { !tmpInputProps.contains(it) }) {
      if (prop.isLateinit && initFun == null)
        throw Exception("For lateinit properties there must be a @LateInit function! - (${sEntity.name}, ${prop.name})")

      val fieldOrRelation = prop.processFieldOrRelation(sEntity, tmpSchema)
      sEntity.addProperty(prop.name, fieldOrRelation)
    }

    // process sealed inheritance
    sealed?.let {
      for (clazz in sealed.value) {
        if (tmpSchema.owned.contains(clazz.qualifiedName))
          throw Exception("Entity already owned! - (${clazz.qualifiedName} -|> ${sEntity.name}) & (${clazz.qualifiedName} owned-by ${tmpSchema.owned[clazz.qualifiedName]})")

        val xEntity = clazz.processEntity(tmpSchema, sEntity.name)
        if (xEntity.type == EntityType.MASTER)
          throw Exception("A master cannot inherit a sealed class! - (${xEntity.name} -|> ${sEntity.name})")

        sEntity.addSealed(xEntity.name, xEntity)
      }
    }

    sEntity
  }
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

    it.constructors.firstOrNull {cst ->
      cst.parameters.isEmpty()
    } ?: throw Exception("Listener '${it.qualifiedName}' requires an empty default constructor!")

    // instantiate listener
    val instance = it.createInstance()
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

private fun KProperty1<Any, *>.processFieldOrRelation(sEntity: SEntity, tmpSchema: TempSchema): SFieldOrRelation {
  if (this.name == "type" || this.name.startsWith("ref") || this.name.startsWith("inv") || this.name.startsWith("traits"))
    throw Exception("Reserved property names: 'type' or starting with 'ref'/'inv'/'traits'! - (${sEntity.name}, ${this.name})")

  val fieldOrRelation = if (this.hasAnnotation<Create>() || this.hasAnnotation<Link>()) {
    this.processRelation(sEntity, tmpSchema)
  } else {
    val type = TypeEngine.convert(this.returnType) ?: throw Exception("Unrecognized field type! - (${sEntity.name}, ${this.name})")
    val checks = this.processChecks()

    SField(this, type, checks, this.returnType.isMarkedNullable)
  }

  if (this.hasAnnotation<Unique>())
    fieldOrRelation.isUnique = true

  // all 'var' properties are also inputs
  if (this is KMutableProperty1<*, *>)
    fieldOrRelation.isInput = true

  return fieldOrRelation
}

private fun KProperty1<Any, *>.processChecks(): Set<SCheck> {
  val checks = findAnnotation<Checks>() ?: return setOf()
  return checks.value.map {
    val sType = it.supertypes.firstOrNull {
      sType -> FieldCheck::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
    } ?: throw Exception("Check '${it.qualifiedName}' must implement '${FieldCheck::class.qualifiedName}'")

    val tRef = sType.arguments.first().type ?: throw Exception("Check '${it.qualifiedName}' requires generic type!")
    if (tRef != this.returnType)
      throw throw Exception("Check '${it.qualifiedName}: ${FieldCheck::class.qualifiedName}<${(tRef.classifier as KClass<*>).simpleName}>' is not compatible with the field type '${(this.returnType.classifier as KClass<*>).simpleName}'!")

    it.constructors.firstOrNull {cst ->
      cst.parameters.isEmpty()
    } ?: throw Exception("Check '${it.qualifiedName}' requires an empty default constructor!")

    // instantiate check
    @Suppress("UNCHECKED_CAST")
    val instance = it.createInstance() as FieldCheck<Any>
    SCheck(it.qualifiedName!!, instance)
  }.toSet()
}

private fun KProperty1<Any, *>.processRelation(sEntity: SEntity, tmpSchema: TempSchema): SRelation {
  this.checkRelationNumber(sEntity.name)

  val type = this.returnType
  val isOpen = this.hasAnnotation<Open>()
  val isOptional = type.isMarkedNullable

  val traits = LinkedHashMap<String, SEntity>()
  val link = this.findAnnotation<Link>()

  val rType = when {
    this.hasAnnotation<Create>() -> RelationType.CREATE
    link != null -> {
      for (trait in link.traits)
        traits[trait.qualifiedName!!] = trait.processEntity(tmpSchema)
      RelationType.LINK
    }
    else -> throw Exception("Required annotation, one of (Create, Link)! - (${sEntity.name}, ${this.name})")
  }

  val (ref, isCollection) = if (type.isSubtypeOf(TypeEngine.LIST) || type.isSubtypeOf(TypeEngine.LIST_TRAITS)) {
    if (sEntity.type == EntityType.TRAIT)
      throw Exception("Collections are not supported in traits! - (${sEntity.name}, ${this.name})")

    if (isOptional)
      throw Exception("Collections cannot be optional! - (${sEntity.name}, ${this.name})")

    val ref = when(rType) {
      RelationType.CREATE -> {
        if (!type.isSubtypeOf(TypeEngine.LIST))
          throw Exception("Create-collection must be of type List<*>! - (${sEntity.name}, ${this.name})")

        var tRef = type.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
        val isPack = if (tRef.isSubtypeOf(TypeEngine.PACK)) {
          tRef = tRef.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
          true
        } else false

        tRef.processCreateRelation(sEntity, this.name, isPack, tmpSchema)
      }

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.LIST_ID))
          throw Exception("Link-collection without traits must be of type List<Long>! - (${sEntity.name}, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.LIST_TRAITS))
          throw Exception("Link-collection with traits type must be of type List<Traits>! - (${sEntity.name}, ${this.name})")

        link!!.value.processEntity(tmpSchema)
      }
    }

    Pair(ref, true)
  } else {
    val ref = when(rType) {
      RelationType.CREATE -> {
        var tRef = type
        val isPack = if (type.isSubtypeOf(TypeEngine.PACK)) {
          tRef = type.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
          true
        } else false

        tRef.processCreateRelation(sEntity, this.name, isPack, tmpSchema)
      }

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.ID))
          throw Exception("Link without traits must be of type, one of (Long, List<Long>)! - (${sEntity.name}, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.TRAITS))
          throw Exception("Link with traits must be of type, one of (Traits, List<Traits>)! - (${sEntity.name}, ${this.name})")

        link!!.value.processEntity(tmpSchema)
      }
    }

    Pair(ref, false)
  }


  if (rType == RelationType.LINK && sEntity.type != EntityType.TRAIT && ref.type == EntityType.TRAIT)
    throw Exception("Cannot link to a trait! Traits do not exist alone. - (${sEntity.name}, ${this.name})")

  if (rType == RelationType.CREATE && ref.type == EntityType.MASTER)
    throw Exception("Cannot create a master through a relation! - (${sEntity.name}, ${this.name})")

  return SRelation(this, rType, ref, traits, isCollection, isOpen, isOptional)
}

private fun KType.processCreateRelation(sEntity: SEntity, rel: String, isPack: Boolean, tmpSchema: TempSchema): SEntity {
  val entity = this.classifier as KClass<*>
  if (tmpSchema.owned.contains(entity.qualifiedName))
    throw Exception("Entity already owned! - (${sEntity.name}, $rel) & (${entity.qualifiedName} owned-by ${tmpSchema.owned[entity.qualifiedName]})")

  val eRef = entity.processEntity(tmpSchema, sEntity.name)

  if (!isPack && eRef.isSealed)
    throw Exception("Create of sealed entity must be of type, one of (Set<Pack<*>>, List<Pack<*>>, Pack<*>)! - (${sEntity.name}, $rel)")

  if (isPack && !eRef.isSealed)
    throw Exception("Create with Pack<*> doesn't correspond to a sealed entity! - (${sEntity.name}, $rel, ${eRef.name})")

  return eRef
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  if (this.hasAnnotation<Trait>()) return EntityType.TRAIT
  return null
}