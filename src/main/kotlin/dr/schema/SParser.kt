package dr.schema

import dr.base.History
import dr.base.Role
import dr.base.User
import dr.state.Machine
import kotlin.reflect.*
import kotlin.reflect.full.*

/* ------------------------- api -------------------------*/
object SParser {
  fun parse(vararg items: KClass<out Any>): Schema {
    println("----Checking Master Entities----")
    val tmpSchema = TempSchema()

    // add internal master entities
    tmpSchema.base[User::class] = User::class.processEntity(tmpSchema)
    tmpSchema.base[Role::class] = Role::class.processEntity(tmpSchema)
    tmpSchema.base[History::class] = History::class.processEntity(tmpSchema)

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
  val base = mutableMapOf<KClass<out Any>, SEntity>()
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
  if (this.hasAnnotation<Own>())
    aNumber++
  
  if (this.hasAnnotation<Link>())
    aNumber++

  if (aNumber > 1)
    throw Exception("KProperty with multiple relation types! Select one of (Own, Link). - ($name, ${this.name})")
}

@Suppress("UNCHECKED_CAST")
private fun KClass<out Any>.processEntity(tmpSchema: TempSchema, ownedBy: String? = null): SEntity {
  val name = this.qualifiedName ?: throw Exception("No Entity name!")
  return tmpSchema.schema.entities.getOrElse(name) {
    this.checkEntityNumber(name)

    if (name.toLowerCase() == "super")
      throw Exception("Reserved class name: 'super'! - ($name)")

    if (this.isOpen || this.isSealed)
      throw Exception("Class inheritance is done via @Sealed. Remove open or sealed keywords. - ($name)")

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
    val sEntity = SEntity(this, type, sealed != null, initFun)

    tmpSchema.schema.addEntity(sEntity)
    if (ownedBy != null)
      tmpSchema.owned[name] = ownedBy

    val tmpInputProps = mutableSetOf<KProperty1<Any, *>>()
    val allProps = memberProperties.map { it.name to (it as KProperty1<Any, *>) }.toMap()

    // all entities have an id
    sEntity.addProperty(ID, SField(ID, null, FieldType.LONG, emptySet(), false))

    // add state-machine if exists
    sEntity.machine = processStateMachine(tmpSchema, sEntity)

    // process ordered inputs
    for (param in primaryConstructor!!.parameters) {
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
      // sealed entities have type
      sEntity.addProperty(TYPE, SField(TYPE, null, FieldType.TEXT, emptySet(), false))

      for (clazz in sealed.value) {
        if (tmpSchema.owned.contains(clazz.qualifiedName))
          throw Exception("Entity already owned! - (${clazz.qualifiedName} -|> ${sEntity.name}) & (${clazz.qualifiedName} owned-by ${tmpSchema.owned[clazz.qualifiedName]})")

        val xEntity = clazz.processEntity(tmpSchema, sEntity.name)
        if (xEntity.type == EntityType.MASTER)
          throw Exception("A master cannot inherit a sealed class! - (${xEntity.name} -|> ${sEntity.name})")

        // sub entities have super reference
        xEntity.addProperty(SUPER, SRelation(SUPER, null, RelationType.OWN, sEntity, emptyMap(), false, false, false))
        sEntity.addSealed(xEntity.name, xEntity)
      }
    }

    sEntity
  }
}

private fun KClass<*>.processStateMachine(tmpSchema: TempSchema, sEntity: SEntity): SMachine? {
  val machine = findAnnotation<StateMachine>()
  return machine?.let {
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Only a @Master can have a state machine! - ($qualifiedName)")

    val machineType = machine.value.supertypes.firstOrNull {
      sType -> Machine::class.qualifiedName == (sType.classifier as KClass<*>).qualifiedName
    } ?: throw Exception("StateMachine '${machine.value.qualifiedName}' must implement '${Machine::class.qualifiedName}'")

    val stateType = machineType.arguments[1].type ?: throw Exception("Machine '${machine.value.qualifiedName}' requires generic types!")
    val stateClass = stateType.classifier as KClass<*>

    val evtType = machineType.arguments.last().type ?: throw Exception("Machine '${machine.value.qualifiedName}' requires generic types!")
    val evtClass = evtType.classifier as KClass<*>
    if (!evtClass.isSealed)
      throw Exception("Machine '${machine.value.qualifiedName}' requires second generic type as a sealed class!")

    machine.value.constructors.firstOrNull {
      it.parameters.isEmpty()
    } ?: throw Exception("StateMachine '${machine.value.qualifiedName}' requires an empty default constructor!")

    val states = stateClass.java.enumConstants.map { it.toString() to (it as Enum<*>) }.toMap()
    val events = evtClass.sealedSubclasses.map { it.qualifiedName!! to it }.toMap()

    if (states.isEmpty())
      throw Exception("StateMachine '${machine.value.qualifiedName}' requires at least one state!")

    // add @state and @open to entity
    sEntity.addProperty(STATE, SField(STATE, null, FieldType.TEXT, emptySet(), false))
    sEntity.addProperty(OPEN, SField(OPEN, null, FieldType.JMAP, emptySet(), false))

    // add History to entity
    val sHistory = tmpSchema.base.getValue(History::class)
    sEntity.addProperty(HISTORY, SRelation(HISTORY, null, RelationType.LINK, sHistory, emptyMap(),true, false, false))

    SMachine(machine.value, states, events)
  }
}

private fun KProperty1<Any, *>.processFieldOrRelation(sEntity: SEntity, tmpSchema: TempSchema): SFieldOrRelation {
  val fieldOrRelation = if (this.hasAnnotation<Own>() || this.hasAnnotation<Link>()) {
    this.processRelation(sEntity, tmpSchema)
  } else {
    val type = TypeEngine.convert(this.returnType) ?: throw Exception("Unrecognized field type! - (${sEntity.name}, ${this.name})")
    val checks = this.processChecks()

    SField(name, this, type, checks, this.returnType.isMarkedNullable)
  }

  if (this.hasAnnotation<Unique>())
    fieldOrRelation.isUnique = true

  // all 'var' properties are also inputs
  if (this is KMutableProperty1<*, *>)
    fieldOrRelation.isInput = true

  return fieldOrRelation
}

private fun KProperty1<Any, *>.processChecks(): Set<SCheck> {
  val checks = findAnnotation<Check>() ?: return setOf()
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

  val type = returnType
  val isOpen = hasAnnotation<Open>()
  val isOptional = type.isMarkedNullable

  val traits = LinkedHashMap<String, SEntity>()
  val link = findAnnotation<Link>()

  val rType = when {
    this.hasAnnotation<Own>() -> RelationType.OWN
    link != null -> {
      for (trait in link.traits)
        traits[trait.qualifiedName!!] = trait.processEntity(tmpSchema)
      RelationType.LINK
    }
    else -> throw Exception("Required annotation, one of (Own, Link)! - (${sEntity.name}, ${this.name})")
  }

  val (ref, isCollection) = if (type.isSubtypeOf(TypeEngine.LIST) || type.isSubtypeOf(TypeEngine.LIST_TRAITS)) {
    if (sEntity.type == EntityType.TRAIT)
      throw Exception("Collections are not supported in traits! - (${sEntity.name}, ${this.name})")

    if (isOptional)
      throw Exception("Collections cannot be optional! - (${sEntity.name}, ${this.name})")

    val ref = when(rType) {
      RelationType.OWN -> {
        if (!type.isSubtypeOf(TypeEngine.LIST))
          throw Exception("Own-collection must be of type List<*>! - (${sEntity.name}, ${this.name})")

        var tRef = type.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
        val isPack = if (tRef.isSubtypeOf(TypeEngine.PACK)) {
          tRef = tRef.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
          true
        } else false

        tRef.processOwnRelation(sEntity, this.name, isPack, tmpSchema)
      }

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.LIST_REFID))
          throw Exception("Link-collection without traits must be of type List<RefID>! - (${sEntity.name}, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.LIST_TRAITS))
          throw Exception("Link-collection with traits type must be of type List<Traits>! - (${sEntity.name}, ${this.name})")

        link!!.value.processEntity(tmpSchema)
      }
    }

    Pair(ref, true)
  } else {
    val ref = when(rType) {
      RelationType.OWN -> {
        var tRef = type
        val isPack = if (type.isSubtypeOf(TypeEngine.PACK_NULL)) {
          tRef = type.arguments.last().type ?: throw Exception("Required generic type! - (${sEntity.name}, ${this.name})")
          true
        } else false

        tRef.processOwnRelation(sEntity, this.name, isPack, tmpSchema)
      }

      RelationType.LINK -> {
        if (traits.isEmpty() && !type.isSubtypeOf(TypeEngine.REFID_NULL))
          throw Exception("Link without traits must be of type, one of (RefID, RefID?, List<RefID>)! - (${sEntity.name}, ${this.name})")

        if (traits.isNotEmpty() && !type.isSubtypeOf(TypeEngine.TRAITS_NULL))
          throw Exception("Link with traits must be of type, one of (Traits, Traits?, List<Traits>)! - (${sEntity.name}, ${this.name})")

        link!!.value.processEntity(tmpSchema)
      }
    }

    Pair(ref, false)
  }


  if (rType == RelationType.LINK && sEntity.type != EntityType.TRAIT && ref.type == EntityType.TRAIT)
    throw Exception("Cannot link to a trait! Traits do not exist alone. - (${sEntity.name}, ${this.name})")

  if (rType == RelationType.OWN && ref.type == EntityType.MASTER)
    throw Exception("Cannot own a master through a relation! - (${sEntity.name}, ${this.name})")

  return SRelation(name, this, rType, ref, traits, isCollection, isOpen, isOptional)
}

private fun KType.processOwnRelation(sEntity: SEntity, rel: String, isPack: Boolean, tmpSchema: TempSchema): SEntity {
  val entity = classifier as KClass<*>
  if (tmpSchema.owned.contains(entity.qualifiedName))
    throw Exception("Entity already owned! - (${sEntity.name}, $rel) & (${entity.qualifiedName} owned-by ${tmpSchema.owned[entity.qualifiedName]})")

  val eRef = entity.processEntity(tmpSchema, sEntity.name)

  if (!isPack && eRef.isSealed)
    throw Exception("Own of sealed entity must be of type, one of (Set<Pack<*>>, List<Pack<*>>, Pack<*>)! - (${sEntity.name}, $rel)")

  if (isPack && !eRef.isSealed)
    throw Exception("Own with Pack<*> doesn't correspond to a sealed entity! - (${sEntity.name}, $rel, ${eRef.name})")

  return eRef
}

private fun KClass<*>.getEntityType(): EntityType? {
  if (this.hasAnnotation<Master>()) return EntityType.MASTER
  if (this.hasAnnotation<Detail>()) return EntityType.DETAIL
  if (this.hasAnnotation<Trait>()) return EntityType.TRAIT
  return null
}