package dr.modification

import dr.DrServer
import dr.schema.*
import dr.spi.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.typeOf

/* ------------------------- api -------------------------*/
sealed class LinkData
  sealed class LinkCreate: LinkData()
    class ManyLinksWithoutTraits(val refs: Collection<Long>): LinkCreate()
    class OneLinkWithoutTraits(val ref: Long): LinkCreate()

    class ManyLinksWithTraits(val refs: Map<Long, Traits>): LinkCreate()
    class OneLinkWithTraits(val ref: Pair<Long, Traits>): LinkCreate()

  sealed class LinkDelete: LinkData()
    class ManyLinkDelete(val links: Collection<Long>): LinkDelete()
    class OneLinkDelete(val link: Long): LinkDelete()


class ModificationEngine(private val adaptor: IModificationAdaptor) {
  private val schema by lazy { DrServer.schema }
  private val tableTranslator by lazy { DrServer.tableTranslator }

  fun create(new: Any): Long {
    val entity = new.javaClass.kotlin.qualifiedName
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - ($entity)")

    // create entity
    val insert = Insert(tableTranslator.getValue(sEntity.name), CreateAction(sEntity))
    val instructions = Instructions(insert)
    checkEntityAndInsert(sEntity, new, instructions, insert)

    // commit instructions
    instructions.fireCheckedListeners()
    val id = adaptor.commit(instructions).first()
    instructions.fireCommittedListeners()

    return id
  }

  fun update(entity: String, id: Long, new: Map<String, Any?>) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")

    // update entity
    val update = Update(sEntity.name, id, UpdateAction(sEntity, id))
    val instructions = Instructions(update)
    checkEntityAndInsert(sEntity, new, instructions, update)

    // commit instructions
    instructions.fireCheckedListeners()
    adaptor.commit(instructions)
    instructions.fireCommittedListeners()
  }

  fun add(entity: String, id: Long, rel: String, new: Any): List<Long> {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.CREATE)
      throw Exception("Relation is not 'create'! - ($entity, $rel)")

    // creates one/many related entities
    val instructions = Instructions()
    val roots = addRelations(sEntity, sRelation, new, instructions, resolvedRef = id)
    instructions.roots.addAll(roots)

    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
  }

  fun link(entity: String, id: Long, rel: String, data: LinkCreate): List<Long> {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    // creates one/many links
    val instructions = Instructions()
    val roots = addLinks(sEntity, sRelation, data, instructions, resolvedRef = id)
    instructions.roots.addAll(roots)

    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
  }

  fun unlink(entity: String, rel: String, data: LinkDelete) {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    // delete one/many links
    val instructions = Instructions()
    removeLinks(sEntity, sRelation, data, instructions)

    // commit instructions
    instructions.fireCheckedListeners()
    adaptor.commit(instructions)
    instructions.fireCommittedListeners()
  }

  fun check(entity: String, field: String, value: Any?) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")

    if (!sField.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, $field)")

    if (!sField.isOptional && value == null)
      throw Exception("Invalid 'null' input! - (${sEntity.name}, $field)")

    value?.let {
      val type =  it.javaClass.kotlin
      if (!TypeEngine.check(sField.type, type))
        throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $field)")

      checkFieldConstraints(sEntity, sField, value)
    }
  }

  private fun checkRelationChange(entity: String, rel: String): Pair<SEntity, SRelation> {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    // check model constraints
    if (!sRelation.isInput)
      throw Exception("Invalid input relation! - ($entity, $rel)")

    if (!sRelation.isCollection)
      throw Exception("Relation is not a collection, use 'update' instead! - ($entity, $rel)")

    if (!sRelation.isOpen)
      throw Exception("Relation is not open, cannot change links! - ($entity, $rel)")

    return sEntity to sRelation
  }

  private fun checkEntityAndInsert(sEntity: SEntity, new: Any, instructions: Instructions, insOrUpd: InsertOrUpdate, include: Boolean = true) {
    // index before adding the instruction (direct references appear before this index)
    val index = if (instructions.size == 0) 0 else instructions.size - 1
    if (include)
      instructions.addInstruction(insOrUpd)

    // get all fields/relations
    val props = sEntity.getAllKeys(insOrUpd, new)

    // --------------------------------- check and process fields ---------------------------------
    for (prop in props) {
      val sField = sEntity.fields[prop] ?: continue // ignore fields that are not part of the schema
      val fValue = new.getFieldValueIfValid(sEntity, sField, insOrUpd is Update)

      // check field constraints if not null
      fValue?.let {
        if (insOrUpd is Update) {
          val type = it.javaClass.kotlin
          if (!TypeEngine.check(sField.type, type))
            throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $prop)")
        }

        checkFieldConstraints(sEntity, sField, fValue)
      }

      // set even if null
      insOrUpd.putData(prop, fValue)
    }

    // --------------------------------- check and process relations ---------------------------------
    for (prop in props) {
      val sRelation = sEntity.rels[prop] ?: continue  // ignore relations that are not part of the schema
      val rValue = new.getRelationValueIfValid(sEntity, sRelation, insOrUpd is Update)

      // check relations constraints and create instruction (insert)
      when (sRelation.type) {
        RelationType.CREATE -> if (!sRelation.isCollection) {
          // TODO: should the ref-entity be deleted when null?
          if (rValue != null) setRelation(sEntity, sRelation, rValue, instructions, insOrUpd, index) else insOrUpd.putResolvedRef("ref_$prop", null)
        } else {
          addRelations(sEntity, sRelation, rValue!!, instructions, insOrUpd)
        }

        RelationType.LINK -> tryCast<LinkData, Unit>(sEntity.name, sRelation.name, rValue!!) {
          when (it) {
            is LinkCreate -> if (!sRelation.isCollection) {
              setLink(sEntity, sRelation, it, instructions, insOrUpd)
            } else {
              addLinks(sEntity, sRelation, it, instructions, insOrUpd)
            }

            is LinkDelete -> removeLinks(sEntity, sRelation, it, instructions)
          }
        }
      }
    }
  }

  private fun setRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate, index: Int) {
    if (insOrUpd is Update) {
      val type = rValue.javaClass.kotlin.qualifiedName
      if (type != sRelation.ref.name)
        throw Exception("Invalid field type, expected ${sRelation.ref.name} found ${type}! - (${sEntity.name}, ${sRelation.name})")
    }

    if (sEntity.type != EntityType.TRAIT) {
      val refInsert = Insert(tableTranslator.getValue(sRelation.ref.name), CreateAction(sRelation.ref))
      checkEntityAndInsert(sRelation.ref, rValue, instructions, refInsert, false)

      // direct references must be inserted before the parent insert
      insOrUpd.putUnresolvedRef("ref_${sRelation.name}", refInsert)
      instructions.addInstruction(index, refInsert)
    } else {
      checkEntityAndInsert(sRelation.ref, rValue, instructions, insOrUpd, false)
    }
  }

  private fun setLink(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, instructions: Instructions, unresolvedRef: InsertOrUpdate) {
    when (data) {
      is OneLinkWithoutTraits -> setLinkNoTraits(sEntity, sRelation, data, instructions, unresolvedRef)
      is OneLinkWithTraits -> setLinkWithTraits(sEntity, sRelation, data, instructions, unresolvedRef)
      else -> throw Exception("Link set doesn't support many-links! - (${sEntity.name}, ${sRelation.name})")
    }
  }

  private fun setLinkNoTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithoutTraits, instructions: Instructions, unresolvedRef: InsertOrUpdate) {
    if (sEntity.type != EntityType.TRAIT) {
      val table = tableTranslator.getValue(sEntity.name)
      Insert("${table}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", data.ref)
        putUnresolvedRef("inv", unresolvedRef)
        instructions.addInstruction(this)
      }
    } else {
      unresolvedRef.putResolvedRef("ref_${sRelation.name}", data.ref)
    }
  }

  private fun setLinkWithTraits(sEntity: SEntity, sRelation: SRelation, data: OneLinkWithTraits, instructions: Instructions, unresolvedRef: InsertOrUpdate) {
    checkTraitsAndInsert(sEntity, sRelation, data.ref.first, data.ref.second, instructions).apply {
      putUnresolvedRef("inv", unresolvedRef)
    }
  }

  private fun addRelations(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): List<Insert> {
    if (unresolvedRef is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    return if (rValue !is Collection<*>) {
      listOf(addOneRelation(sEntity, sRelation, rValue, instructions, unresolvedRef, resolvedRef))
    } else {
      tryCast<Collection<Any>, List<Insert>>(sEntity.name, sRelation.name, rValue) { col ->
        col.map { addOneRelation(sEntity, sRelation, it, instructions, unresolvedRef, resolvedRef) }
      }
    }
  }

  private fun addOneRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    val invTable = tableTranslator.getValue(sEntity.name)
    val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name), AddAction(sEntity, sRelation)).apply {
      if (resolvedRef == null) putUnresolvedRef("inv_${invTable}_${sRelation.name}", unresolvedRef!!) else putResolvedRef("inv_${invTable}_${sRelation.name}", resolvedRef)
    }

    checkEntityAndInsert(sRelation.ref, rValue, instructions, childInsert)
    return childInsert
  }

  private fun addLinks(sEntity: SEntity, sRelation: SRelation, data: LinkCreate, instructions: Instructions, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): List<Insert> {
    if (unresolvedRef is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // TODO: should traits support collections? (more tests required!) - change schema support

    return when (data) {
      is OneLinkWithoutTraits -> listOf(addOneLinkNoTraits(sEntity, sRelation, data.ref, instructions, unresolvedRef, resolvedRef))
      is ManyLinksWithoutTraits -> data.refs.map { addOneLinkNoTraits(sEntity, sRelation, it, instructions, unresolvedRef, resolvedRef) }
      is OneLinkWithTraits -> listOf(addOneLinkWithTraits(sEntity, sRelation, data.ref, instructions, unresolvedRef, resolvedRef))
      is ManyLinksWithTraits -> data.refs.map { addOneLinkWithTraits(sEntity, sRelation, it.toPair(), instructions, unresolvedRef, resolvedRef) }
    }
  }

  private fun addOneLinkNoTraits(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    val table = tableTranslator.getValue(sEntity.name)
    return Insert("${table}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      if (resolvedRef == null) putUnresolvedRef("inv", unresolvedRef!!) else putResolvedRef("inv", resolvedRef)
      instructions.addInstruction(this)
    }
  }

  private fun addOneLinkWithTraits(sEntity: SEntity, sRelation: SRelation, ref: Pair<Long, Traits>, instructions: Instructions, unresolvedRef: InsertOrUpdate? = null, resolvedRef: Long? = null): Insert {
    return checkTraitsAndInsert(sEntity, sRelation, ref.first, ref.second, instructions).apply {
      if (resolvedRef == null) putUnresolvedRef("inv", unresolvedRef!!) else putResolvedRef("inv", resolvedRef)
    }
  }

  private fun removeLinks(sEntity: SEntity, sRelation: SRelation, data: LinkDelete, instructions: Instructions) {
    if (sRelation.isCollection && !sRelation.isOpen)
      throw Exception("Relation is not open, cannot remove links! - (${sEntity.name}, ${sRelation.name})")

    if (!sRelation.isCollection && !sRelation.isOptional)
      throw Exception("Relation is not optional, cannot remove links! - (${sEntity.name}, ${sRelation.name})")

    when (data) {
      is OneLinkDelete -> removeOneLink(sEntity, sRelation, data.link, instructions)
      is ManyLinkDelete -> data.links.forEach { removeOneLink(sEntity, sRelation, it, instructions) }
    }
  }

  private fun removeOneLink(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions) {
    val table = tableTranslator.getValue(sEntity.name)
    Delete("${table}__${sRelation.name}", link, UnlinkAction(sEntity, sRelation)).apply {
      instructions.addInstruction(this)
    }
  }

  private fun checkTraitsAndInsert(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Traits, instructions: Instructions): Insert {
    if (sRelation.traits.size != traits.traits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    val table = tableTranslator.getValue(sEntity.name)
    val traitInsert = Insert("${table}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      instructions.addInstruction(this)
    }

    val processedTraits = mutableSetOf<Any>()
    for (trait in traits.traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      processedTraits.add(sTrait)
      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      // Does not insert. Compact all trait fields
      checkEntityAndInsert(sTrait, trait, instructions, traitInsert, false)
    }

    if (sRelation.traits.size != processedTraits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    return traitInsert
  }
}


/* ------------------------- helpers -------------------------*/
private fun checkFieldConstraints(sEntity: SEntity, sField: SField, value: Any) {
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
      is Pair<*, *> -> OneLinkWithTraits(data as Pair<Long, Traits>)
      is Collection<*> -> ManyLinksWithoutTraits(data as Collection<Long>)
      is Map<*, *> -> ManyLinksWithTraits(data as Map<Long, Traits>)
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