package dr.modification

import dr.schema.*
import dr.spi.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.typeOf

class InstructionBuilder(private val schema: Schema, private val tableTranslator: Map<String, String>) {
  private val instructions = Instructions()

  fun build() = instructions

  fun addRoot(root: Instruction) = instructions.roots.add(root)
  fun addRoots(roots: List<Instruction>) = instructions.roots.addAll(roots)

  fun createEntity(data: Any): Insert {
    val (master, extended) = if (data is Pack) {
      if (data.values.isEmpty())
        throw Exception("Pack must contain values!")
      Pair(data.values.first(), data.values.drop(1))
    } else {
      Pair(data, listOf<SEntity>())
    }

    val mEntity = schema.findEntity(master)
    if (mEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - (${mEntity.name})")

    // create master entity
    val mInsert = Insert(table(mEntity), CreateAction(mEntity))
    checkEntityAndInsert(mEntity, mInsert, master)

    // create extended entities if exist
    var tInsert = mInsert
    for (item in extended) {
      tInsert = createSealed(tInsert, item)
    }

    // sealed chain must be complete!
    if (tInsert.action.sEntity.isSealed)
      throw Exception("Creation of sealed entity must contain extended entities! - (${tInsert.action.sEntity.name})")

    return mInsert
  }

  fun updateEntity(sEntity: SEntity, id: Long, data: Map<String, Any?>): Update {
    val update = Update(table(sEntity), id, UpdateAction(sEntity, id))
    checkEntityAndInsert(sEntity, update, data)
    return update
  }

  private fun createSealed(topInst: Insert, value: Any): Insert {
    val sTop = topInst.action.sEntity
    val sEntity = schema.findEntity(value)
    if (!sTop.isSealed)
      throw Exception("Not a top @Sealed entity! - (${sTop.name})")

    topInst.putData("type", sEntity.name)
    return Insert(table(sEntity), CreateAction(sEntity)).apply {
      checkEntityAndInsert(sEntity,this, value)
      this.putUnresolvedRef("ref_type", topInst)
    }
  }

  private fun checkEntityAndInsert(sEntity: SEntity, topInst: InsertOrUpdate, new: Any, include: Boolean = true) {
    // index before adding the instruction (direct references appear before this index)
    val index = if (instructions.size == 0) 0 else instructions.size - 1
    if (include)
      instructions.addInstruction(topInst)

    // get all fields/relations
    val props = sEntity.getAllKeys(topInst, new)

    // --------------------------------- check and process fields ---------------------------------
    for (prop in props) {
      val sField = sEntity.fields[prop] ?: continue // ignore fields that are not part of the schema
      val fValue = new.getFieldValueIfValid(sEntity, sField, topInst is Update)

      // check field constraints if not null
      fValue?.let {
        if (topInst is Update) {
          val type = it.javaClass.kotlin
          if (!TypeEngine.check(sField.type, type))
            throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $prop)")
        }

        checkFieldConstraints(sEntity, sField, fValue)
      }

      // set even if null
      topInst.putData(prop, fValue)
    }

    // --------------------------------- check and process relations ---------------------------------
    for (prop in props) {
      val sRelation = sEntity.rels[prop] ?: continue  // ignore relations that are not part of the schema
      val rValue = new.getRelationValueIfValid(sEntity, sRelation, topInst is Update)

      // check relations constraints and create instruction (insert)
      when (sRelation.type) {
        RelationType.CREATE -> if (!sRelation.isCollection) {
          // TODO: should the ref-entity be deleted when null?
          if (rValue != null) setRelation(sEntity, sRelation, topInst, rValue, index) else topInst.putResolvedRef("ref_$prop", null)
        } else {
          addRelations(sEntity, sRelation, rValue!!, topInst)
        }

        RelationType.LINK -> tryCast<LinkData, Unit>(sEntity.name, sRelation.name, rValue!!) {
          when (it) {
            is LinkCreate -> if (!sRelation.isCollection) {
              setLink(sEntity, sRelation, it, topInst)
            } else {
              addLinks(sEntity, sRelation, it, topInst)
            }

            is LinkDelete -> removeLinks(sEntity, sRelation, it)
          }
        }
      }
    }
  }

  private fun setRelation(sEntity: SEntity, sRelation: SRelation, topInst: InsertOrUpdate, rValue: Any, index: Int) {
    if (topInst is Update) {
      val type = rValue.javaClass.kotlin.qualifiedName
      if (type != sRelation.ref.name)
        throw Exception("Invalid field type, expected ${sRelation.ref.name} found ${type}! - (${sEntity.name}, ${sRelation.name})")
    }

    if (sEntity.type != EntityType.TRAIT) {
      val refInsert = Insert(table(sRelation.ref), CreateAction(sRelation.ref))
      checkEntityAndInsert(sRelation.ref, refInsert, rValue, false)

      // direct references must be inserted before the parent insert
      topInst.putUnresolvedRef("ref_${sRelation.name}", refInsert)
      instructions.addInstruction(index, refInsert)
    } else {
      checkEntityAndInsert(sRelation.ref, topInst, rValue,false)
    }
  }

  private fun setLink(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, unresolvedRef: InsertOrUpdate) {
    when (data) {
      is OneLinkWithoutTraits -> setLinkNoTraits(sEntity, sRelation, data, unresolvedRef)
      is OneLinkWithTraits -> setLinkWithTraits(sEntity, sRelation, data, unresolvedRef)
      else -> throw Exception("Link set doesn't support many-links! - (${sEntity.name}, ${sRelation.name})")
    }
  }

  private fun setLinkNoTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithoutTraits, unresolvedRef: InsertOrUpdate) {
    if (sEntity.type != EntityType.TRAIT) {
      Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", data.ref)
        putUnresolvedRef("inv", unresolvedRef)
        instructions.addInstruction(this)
      }
    } else {
      unresolvedRef.putResolvedRef("ref_${sRelation.name}", data.ref)
    }
  }

  private fun setLinkWithTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithTraits, unresolvedRef: InsertOrUpdate) {
    checkTraitsAndInsert(sEntity, sRelation, data.ref, data.traits).apply {
      putUnresolvedRef("inv", unresolvedRef)
    }
  }

  fun addRelations(sEntity: SEntity, sRelation: SRelation, rValue: Any, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): List<Insert> {
    if (unresolvedRef is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    return if (rValue !is Collection<*>) {
      listOf(addOneRelation(sEntity, sRelation, rValue, unresolvedRef, resolvedRef))
    } else {
      tryCast<Collection<Any>, List<Insert>>(sEntity.name, sRelation.name, rValue) { col ->
        col.map { addOneRelation(sEntity, sRelation, it, unresolvedRef, resolvedRef) }
      }
    }
  }

  private fun addOneRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    val invTable = table(sEntity)
    val childInsert = Insert(table(sRelation.ref), AddAction(sEntity, sRelation)).apply {
      if (resolvedRef == null) putUnresolvedRef("inv_${invTable}_${sRelation.name}", unresolvedRef!!) else putResolvedRef("inv_${invTable}_${sRelation.name}", resolvedRef)
    }

    checkEntityAndInsert(sRelation.ref, childInsert, rValue)
    return childInsert
  }

  fun addLinks(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): List<Insert> {
    if (unresolvedRef is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // this is also checked in the SParser!!
    if (sEntity.type == EntityType.TRAIT)
      throw Exception("Collections are not supported in traits! - (${sEntity.name}, ${sRelation.name})")

    return when (data) {
      is OneLinkWithoutTraits -> listOf(addOneLinkNoTraits(sEntity, sRelation, data.ref, unresolvedRef, resolvedRef))
      is ManyLinksWithoutTraits -> data.refs.map { addOneLinkNoTraits(sEntity, sRelation, it, unresolvedRef, resolvedRef) }
      is OneLinkWithTraits -> listOf(addOneLinkWithTraits(sEntity, sRelation, data.ref, data.traits, unresolvedRef, resolvedRef))
      is ManyLinksWithTraits -> data.refs.map { addOneLinkWithTraits(sEntity, sRelation, it.key, it.value, unresolvedRef, resolvedRef) }
    }
  }

  private fun addOneLinkNoTraits(sEntity: SEntity, sRelation: SRelation, link: Long, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    return Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      if (resolvedRef == null) putUnresolvedRef("inv", unresolvedRef!!) else putResolvedRef("inv", resolvedRef)
      instructions.addInstruction(this)
    }
  }

  private fun addOneLinkWithTraits(sEntity: SEntity, sRelation: SRelation, ref: Long, traits: Pack, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    return checkTraitsAndInsert(sEntity, sRelation, ref, traits).apply {
      if (resolvedRef == null) putUnresolvedRef("inv", unresolvedRef!!) else putResolvedRef("inv", resolvedRef)
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

  private fun removeOneLink(sEntity: SEntity, sRelation: SRelation, link: Long) {
    Delete("${table(sEntity)}__${sRelation.name}", link, UnlinkAction(sEntity, sRelation)).apply {
      instructions.addInstruction(this)
    }
  }

  private fun checkTraitsAndInsert(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Pack): Insert {
    if (sRelation.traits.size != traits.values.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.values.size}'! - (${sEntity.name}, ${sRelation.name})")

    val traitInsert = Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      instructions.addInstruction(this)
    }

    val processedTraits = mutableSetOf<Any>()
    for (trait in traits.values) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      processedTraits.add(sTrait)
      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      // Does not insert. Compact all trait fields
      checkEntityAndInsert(sTrait, traitInsert, trait,false)
    }

    if (sRelation.traits.size != processedTraits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.values.size}'! - (${sEntity.name}, ${sRelation.name})")

    return traitInsert
  }

  private fun table(sEntity: SEntity) = tableTranslator.getValue(sEntity.name)
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
private fun SEntity.getAllKeys(insOrUpd: InsertOrUpdate, new: Any): Set<String> {
  return if (insOrUpd is Update) {
    if (new !is Map<*, *>)
      throw Exception("Expected Map<String, Any?> of values for update! - (${this.name})")

    (new as Map<String, *>).keys
  } else {
    // check if value is of correct type
    val vType = new.javaClass.kotlin.qualifiedName
    if (vType != this.name)
      throw Exception("Invalid input type, expected ${this.name} found ${vType}!")

    new.javaClass.kotlin.memberProperties.map { it.name }.toSet()
  }
}

private fun Any.getFieldValueIfValid(sEntity: SEntity, sField: SField, isUpdate: Boolean): Any? {
  val fValue = if (isUpdate) {
    if (!sField.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sField.name})")

    tryCast<Map<String, Any?>, Any?>(sEntity.name, sField.name, this) { it[sField.name] }
  } else {
    sField.getValue(this)
  }

  if (!sField.isOptional && fValue == null)
    throw Exception("Invalid 'null' input! - (${sEntity.name}, ${sField.name})")

  return fValue
}

private fun Any.getRelationValueIfValid(sEntity: SEntity, sRelation: SRelation, isUpdate: Boolean): Any? {
  val rValue = if (isUpdate) {
    if (!sRelation.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, ${sRelation.name})")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - (${sEntity.name}, ${sRelation.name})")

    tryCast<Map<String, Any?>, Any?>(sEntity.name, sRelation.name, this) { it[sRelation.name] }
  } else {
    sRelation.getValue(this).translate(sRelation)
  }

  if (!sRelation.isOptional && rValue == null)
    throw Exception("Invalid 'null' input! - (${sEntity.name}, ${sRelation.name})")

  return rValue
}

@Suppress("UNCHECKED_CAST")
private fun Any?.translate(sRelation: SRelation): Any? {
  return if (sRelation.type == RelationType.LINK) {
    when (val data = this!!) {
      is Long -> OneLinkWithoutTraits(data)
      is Pair<*, *> -> OneLinkWithTraits(data.first as Long, data.second as Pack)
      is Collection<*> -> ManyLinksWithoutTraits(data as Collection<Long>)
      is Map<*, *> -> ManyLinksWithTraits(data as Map<Long, Pack>)
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
