package dr.modification

import dr.schema.*
import dr.spi.*
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.typeOf

class InstructionBuilder(private val schema: Schema, private val tableTranslator: Map<String, String>) {
  private val instructions = Instructions()

  fun build() = instructions
  fun addRoot(root: Instruction) = instructions.roots.add(root)
  fun addRoots(roots: List<Instruction>) = instructions.roots.addAll(roots)
  private fun addInstruction(inst: Instruction) = instructions.list.add(inst)
  private fun addInstruction(index: Int, inst: Instruction) = instructions.list.add(index, inst)

  fun createEntity(data: Any): Insert {
    val mEntity = schema.findTopEntity(data)
    if (mEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - (${mEntity.name})")

    // create master and sealed children
    return Insert(table(mEntity), CreateAction(mEntity)).apply {
      addInstruction(this)
      checkAnyAndInsert(data,this)
    }
  }

  fun updateEntity(sEntity: SEntity, id: Long, data: Map<String, Any?>): Update {
    return Update(table(sEntity), id, UpdateAction(sEntity, id)).apply {
      addInstruction(this)
      checkEntityAndInsert(sEntity, data, this)
    }
  }

  fun addRelations(sEntity: SEntity, sRelation: SRelation, rValue: Any, unresolvedInv: Insert? = null, resolvedInv: Long? = null): List<Insert> {
    return if (rValue !is Collection<*>) {
      listOf(addOneRelation(sEntity, sRelation, rValue, unresolvedInv, resolvedInv))
    } else {
      tryCast<Collection<Any>, List<Insert>>(sEntity.name, sRelation.name, rValue) { col ->
        col.map { addOneRelation(sEntity, sRelation, it, unresolvedInv, resolvedInv) }
      }
    }
  }

  fun addLinks(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, unresolvedInv: Insert? = null, resolvedInv: Long? = null): List<InsertOrUpdate> {
    // this is also checked in SParser!
    if (sEntity.type == EntityType.TRAIT)
      throw Exception("Collections are not supported in traits! - (${sEntity.name}, ${sRelation.name})")

    return when (data) {
      is OneLinkWithoutTraits -> listOf(addLinkNoTraits(sEntity, sRelation, data.ref, unresolvedInv, resolvedInv))
      is ManyLinksWithoutTraits -> data.refs.map { addLinkNoTraits(sEntity, sRelation, it, unresolvedInv, resolvedInv) }
      is OneLinkWithTraits -> listOf(addLinkWithTraits(sEntity, sRelation, data.ref, unresolvedInv, resolvedInv))
      is ManyLinksWithTraits -> data.refs.map { addLinkWithTraits(sEntity, sRelation, it, unresolvedInv, resolvedInv) }
    }
  }

  fun removeLinks(sEntity: SEntity, sRelation: SRelation, data: LinkDelete) {
    if (sRelation.isCollection && !sRelation.isOpen)
      throw Exception("Relation is not open, cannot remove links! - (${sEntity.name}, ${sRelation.name})")

    if (!sRelation.isCollection && !sRelation.isOptional)
      throw Exception("Relation is not optional, cannot remove links! - (${sEntity.name}, ${sRelation.name})")

    when (data) {
      is OneLinkDelete -> removeOneLink(sEntity, sRelation, data.link)
      is ManyLinkDelete -> data.links.forEach { removeOneLink(sEntity, sRelation, it) }
    }
  }


  /* ------------------------------------------------------------ private ------------------------------------------------------------ */
  private fun table(sEntity: SEntity) = tableTranslator.getValue(sEntity.name)

  private fun checkAnyAndInsert(data: Any, topInst: InsertOrUpdate) {
    val (head, tail) = if (data is Pack<*>) {
      Pair(data.head, data.tail)
    } else {
      Pair(data, emptyArray())
    }

    val sEntity = schema.findEntity(head)
    checkPackAndInsert(sEntity, head, tail, topInst)
  }

  private fun checkPackAndInsert(topEntity: SEntity, head: Any, tail: Array<out Any>, topInst: InsertOrUpdate) {
    // create top entity
    checkEntityAndInsert(topEntity, head, topInst)

    // create extended entities if exist
    var nextEntity = topEntity
    var nextInst = topInst
    for (item in tail) {
      nextInst = createSealed(topEntity, item, nextInst)
      nextEntity = nextInst.action.sEntity
    }

    // sealed chain must be complete!
    if (nextEntity.isSealed)
      throw Exception("Creation of sealed entity must contain extended entities! - (${nextEntity.name})")
  }

  private fun createSealed(topEntity: SEntity, value: Any, topInst: InsertOrUpdate): Insert {
    val sEntity = schema.findEntity(value)
    if (!topEntity.isSealed)
      throw Exception("Not a top @Sealed entity! - (${topEntity.name})")

    topInst.putData("type", sEntity.name)
    return Insert(table(sEntity), CreateAction(sEntity)).apply {
      putUnresolvedRef("ref_super", topInst)
      addInstruction(this)
      checkEntityAndInsert(sEntity, value, this)
    }
  }

  private fun checkEntityAndInsert(sEntity: SEntity, new: Any, topInst: InsertOrUpdate) {
    // index the current instruction (direct references appear before this index)
    val index = instructions.size - 1

    val isUpdate = new is Map<*, *>

    //invoke @LateInit function if exists
    if (!isUpdate)
      sEntity.initFun?.call(new)

    // get all fields/relations
    val props = sEntity.getAllKeys(new)

    // --------------------------------- check and process fields ---------------------------------
    for (prop in props) {
      val sField = sEntity.fields[prop] ?: continue // ignore fields that are not part of the schema
      val fValue = new.getFieldValueIfValid(sEntity, sField)
      fValue?.let { checkFieldConstraints(sEntity, sField, fValue) }
      topInst.putData(prop, fValue)
    }

    // --------------------------------- check and process relations ---------------------------------
    for (prop in props) {
      val sRelation = sEntity.rels[prop] ?: continue  // ignore relations that are not part of the schema
      val rValue = new.getRelationValueIfValid(sEntity, sRelation)

      // check relations constraints and create instruction (insert)
      when (sRelation.type) {
        RelationType.CREATE -> if (!sRelation.isCollection) {
          // TODO: should the ref-entity be deleted when null?
          if (rValue != null) setReference(sEntity, sRelation, rValue, topInst, index) else topInst.putResolvedRef("ref__$prop", null)
        } else {
          if (topInst is Update)
            throw Exception("Cannot update collections, use 'add/link/unlink' instead! - (${sEntity.name}, ${sRelation.name})")
          addRelations(sEntity, sRelation, rValue!!, topInst as Insert)
        }

        RelationType.LINK -> tryCast<LinkData, Unit>(sEntity.name, sRelation.name, rValue!!) {
          when (it) {
            is LinkCreate -> if (!sRelation.isCollection) {
              setLink(sEntity, sRelation, it, topInst)
            } else {
              if (topInst is Update)
                throw Exception("Cannot update collections, use 'add/link/unlink' instead! - (${sEntity.name}, ${sRelation.name})")
              addLinks(sEntity, sRelation, it, topInst as Insert)
            }

            is LinkDelete -> removeLinks(sEntity, sRelation, it)
          }
        }
      }
    }
  }

  /* ------------------------------------------------------------ one-to-one ------------------------------------------------------------ */
  private fun setReference(sEntity: SEntity, sRelation: SRelation, rValue: Any, topInst: InsertOrUpdate, index: Int) {
    if (topInst is Update) {
      val type = rValue.javaClass.kotlin.qualifiedName
      if (type != sRelation.ref.name)
        throw Exception("Invalid field type, expected ${sRelation.ref.name} found ${type}! - (${sEntity.name}, ${sRelation.name})")
    }

    if (sEntity.type == EntityType.TRAIT || sRelation.ref.type == EntityType.TRAIT) {
      // unwrap properties (reuse instruction)
      topInst.with(sEntity.type != EntityType.TRAIT, sRelation.name) {
        checkAnyAndInsert(rValue, topInst)
      }
    } else {
      // A ref_<rel> --> B
      Insert(table(sRelation.ref), CreateAction(sRelation.ref)).apply {
        topInst.putUnresolvedRef("ref__${sRelation.name}", this)
        addInstruction(index,this)
        checkAnyAndInsert(rValue, this)
      }
    }
  }

  private fun setLink(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, topInst: InsertOrUpdate) {
    when (data) {
      is OneLinkWithoutTraits -> setLinkNoTraits(sEntity, sRelation, data, topInst)
      is OneLinkWithTraits -> setLinkWithTraits(sEntity, sRelation, data, topInst)
      else -> throw Exception("Link set doesn't support many-links! - (${sEntity.name}, ${sRelation.name})")
    }
  }

  private fun setLinkNoTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithoutTraits, topInst: InsertOrUpdate) {
    topInst.putResolvedRef("ref__${sRelation.name}", data.ref)

    /*if (sEntity.type == EntityType.TRAIT || sRelation.ref.type == EntityType.TRAIT) {
      // unwrap properties (reuse instruction)
      topInst.putResolvedRef("ref__${sRelation.name}", data.ref)
    } else {
      // A <-- [inv . ref] --> B
      Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", data.ref)
        putUnresolvedRef("inv", topInst)
        addInstruction(this)
      }
    }*/
  }

  private fun setLinkWithTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithTraits, topInst: InsertOrUpdate) {
    topInst.putResolvedRef("ref__${sRelation.name}", data.ref.id)

    topInst.with(sEntity.type != EntityType.TRAIT, "traits__${sRelation.name}") {
      unwrapTraits(sEntity, sRelation, data.ref, topInst)
    }

    /*if (sRelation.ref.type == EntityType.TRAIT) {
      // unwrap properties (reuse instruction)
      topInst.putResolvedRef("ref__${sRelation.name}", data.ref)

      topInst.with(sEntity.type != EntityType.TRAIT, sRelation.name) {
        unwrapTraits(sEntity, sRelation, data.traits, topInst)
      }

    } else {
      val linkTable = addLinkRow(sEntity, sRelation, data.ref, topInst)
      unwrapTraits(sEntity, sRelation, data.traits, linkTable)
    }*/
  }


  /* ------------------------------------------------------------ one-to-many ------------------------------------------------------------ */
  private fun addOneRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, unresolvedInv: InsertOrUpdate? = null, resolvedInv: Long? = null): Insert {
    // TODO: to support this we need to return a topInst (but it doesn't always exist)
    if (sRelation.ref.type == EntityType.TRAIT)
      throw Exception("Doesn't support collections of traits yet!")

    // A <-- inv_<A>_<rel> B
    val invTable = table(sEntity)
    return Insert(table(sRelation.ref), AddAction(sEntity, sRelation)).apply {
      if (resolvedInv == null) putUnresolvedRef("inv_${invTable}__${sRelation.name}", unresolvedInv!!) else putResolvedRef("inv_${invTable}__${sRelation.name}", resolvedInv)
      addInstruction(this)
      checkAnyAndInsert(rValue, this)
    }
  }

  private fun addLinkNoTraits(sEntity: SEntity, sRelation: SRelation, link: Long, unresolvedInv: InsertOrUpdate? = null, resolvedInv: Long? = null): InsertOrUpdate {
    // TODO: to support this we need to return a topInst (but it doesn't always exist)
    if (sRelation.ref.type == EntityType.TRAIT)
      throw Exception("Doesn't support collections of traits yet!")

    return if (sRelation.isUnique) {
      // A <-- inv_<A>_<rel> B
      val invTable = table(sEntity)
      Update(table(sRelation.ref), link, LinkAction(sEntity, sRelation)).apply {
        if (resolvedInv == null) putUnresolvedRef("inv_${invTable}__${sRelation.name}", unresolvedInv!!) else putResolvedRef("inv_${invTable}__${sRelation.name}", resolvedInv)
        addInstruction(this)
      }
    } else {
      addLinkRow(sEntity, sRelation, link, unresolvedInv, resolvedInv)
    }
  }

  private fun addLinkWithTraits(sEntity: SEntity, sRelation: SRelation, traits: Traits, unresolvedInv: InsertOrUpdate? = null, resolvedInv: Long? = null): InsertOrUpdate {
    // TODO: to support this we need to return a topInst (but it doesn't always exist)
    if (sRelation.ref.type == EntityType.TRAIT)
      throw Exception("Doesn't support collections of traits yet!")

    val linkTable = addLinkRow(sEntity, sRelation, traits.id, unresolvedInv, resolvedInv)

    unwrapTraits(sEntity, sRelation, traits, linkTable)
    return linkTable
  }

  private fun addLinkRow(sEntity: SEntity, sRelation: SRelation, link: Long, unresolvedInv: InsertOrUpdate? = null, resolvedInv: Long? = null): Insert {
    // A <-- [inv <traits> ref] --> B
    return Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      if (resolvedInv == null) putUnresolvedRef("inv", unresolvedInv!!) else putResolvedRef("inv", resolvedInv)
      addInstruction(this)
    }
  }

  private fun unwrapTraits(sEntity: SEntity, sRelation: SRelation, traits: Traits, linkTable: InsertOrUpdate) {
    if (sRelation.traits.size != traits.traits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    val processedTraits = mutableSetOf<Any>()
    for (trait in traits.traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      processedTraits.add(sTrait)
      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      // Does not insert. Compact all trait fields
      checkAnyAndInsert(trait, linkTable)
    }

    if (sRelation.traits.size != processedTraits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")
  }

  private fun removeOneLink(sEntity: SEntity, sRelation: SRelation, link: Long) {
    Delete("${table(sEntity)}__${sRelation.name}", link, UnlinkAction(sEntity, sRelation)).apply {
      addInstruction(this)
    }
  }
}



/* ------------------------- helpers -------------------------*/
fun checkFieldConstraints(sEntity: SEntity, sField: SField, value: Any) {
  sField.checks.forEach {
    it.check(value)?.let { msg ->
      throw Exception("Constraint check failed '$msg'! - (${sEntity.name}, ${sField.name})")
    }
  }
}

@Suppress("UNCHECKED_CAST")
private fun SEntity.getAllKeys(new: Any): Set<String> {
  return if (new is Map<*, *>) {
    (new as Map<String, *>).keys
  } else {
    val vType = new.javaClass.kotlin.qualifiedName
    if (vType != this.name)
      throw Exception("Invalid input type, expected ${this.name} found ${vType}!")

    new.javaClass.kotlin.memberProperties.map { it.name }.toSet()
  }

  /*val vType = new.javaClass.kotlin.qualifiedName
  return if (isUpdate) {
    if (new !is Map<*, *>)
      throw Exception("Expected Map<String, Any?> of values for update, found ${vType}! - (${this.name})")

    (new as Map<String, *>).keys
  } else {
    if (vType != this.name)
      throw Exception("Invalid input type, expected ${this.name} found ${vType}!")

    new.javaClass.kotlin.memberProperties.map { it.name }.toSet()
  }*/
}

private fun Any.getFieldValueIfValid(sEntity: SEntity, sField: SField): Any? {
  val fValue = if (this is Map<*, *>) {
    if (!sField.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sField.name})")

    val value = tryCast<Map<String, Any?>, Any?>(sEntity.name, sField.name, this) { it[sField.name] }
    value?.let {
      val vType = value.javaClass.kotlin
      if (!TypeEngine.check(sField.type, vType))
        throw Exception("Invalid field type, expected ${sField.type} found ${vType.simpleName}! - (${sEntity.name}, ${sField.name})")
    }

    value
  } else {
    sField.getValue(this)
  }

  /*val fValue = if (isUpdate) {
    if (!sField.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sField.name})")

    tryCast<Map<String, Any?>, Any?>(sEntity.name, sField.name, this) { it[sField.name] }
  } else {
    sField.getValue(this)
  }*/

  if (!sField.isOptional && fValue == null)
    throw Exception("Invalid 'null' input! - (${sEntity.name}, ${sField.name})")

  return fValue
}

private fun Any.getRelationValueIfValid(sEntity: SEntity, sRelation: SRelation): Any? {
  val rValue = if (this is Map<*, *>) {
    if (!sRelation.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sRelation.name})")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - (${sEntity.name}, ${sRelation.name})")

    tryCast<Map<String, Any?>, Any?>(sEntity.name, sRelation.name, this) { it[sRelation.name] }
  } else {
    sRelation.getValue(this).translate(sRelation)
  }

  /*val rValue = if (isUpdate) {
    if (!sRelation.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sRelation.name})")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - (${sEntity.name}, ${sRelation.name})")

    tryCast<Map<String, Any?>, Any?>(sEntity.name, sRelation.name, this) { it[sRelation.name] }
  } else {
    sRelation.getValue(this).translate(sRelation)
  }*/

  if (!sRelation.isOptional && rValue == null)
    throw Exception("Invalid 'null' input! - (${sEntity.name}, ${sRelation.name})")

  return rValue
}

@Suppress("UNCHECKED_CAST")
private fun Any?.translate(sRelation: SRelation): Any? {
  return if (sRelation.type == RelationType.LINK) {
    when (val data = this!!) {
      is Long -> OneLinkWithoutTraits(data)
      is Traits -> OneLinkWithTraits(data)
      is Collection<*> -> {
        if (data.javaClass.kotlin.createType().arguments.last().type!!.isSubtypeOf(typeOf<Long>())) {
          ManyLinksWithoutTraits(data as Collection<Long>)
        } else {
          ManyLinksWithTraits(data as Collection<Traits>)
        }
      }

      else -> throw  Exception("Unable to translate link '${data.javaClass.kotlin.qualifiedName}'! - (Code bug, please report the issue)")
    }
  } else {
    this
  }
}

private inline fun <reified T, R> tryCast(entity: String, field: String, value: Any, exec: (T) -> R): R {
  val expectedType = typeOf<T>()
  val foundType = value.javaClass.kotlin.qualifiedName

  try {
    return exec(value as T)
  } catch (ex: ClassCastException) {
    throw Exception("Invalid field type, expected $expectedType found $foundType! - ($entity, $field)")
  }
}
