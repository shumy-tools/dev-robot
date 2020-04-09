package dr.modification

import dr.DrServer
import dr.schema.*
import dr.spi.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.typeOf

/* ------------------------- api -------------------------*/
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
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    // check model constraints
    if (!sRelation.isInput)
      throw Exception("Invalid input relation! - (${sEntity.name}, $rel)")

    if (!sRelation.isCollection)
      throw Exception("Relation is not a collection! - ($entity, $rel)")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - ($entity, $rel)")

    if (sRelation.type != RelationType.CREATE)
      throw Exception("Relation is not 'create'! - ($entity, $rel)")

    // creates one/many related entities
    val instructions = Instructions()

    // supports new as Entity or Collection<Entity>
    val children = addRelations(sEntity, sRelation, new, instructions, invRef = id)
    instructions.roots.addAll(children)

    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
  }

  fun link(entity: String, id: Long, rel: String, new: Any): List<Long> {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    // check model constraints
    if (!sRelation.isInput)
      throw Exception("Invalid input relation! - (${sEntity.name}, $rel)")

    if (!sRelation.isCollection)
      throw Exception("Relation is not a collection! - ($entity, $rel)")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - ($entity, $rel)")

    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    // creates one/many links
    val instructions = Instructions()
    val children = if (sRelation.traits.isEmpty()) {
      // supports new as Long or Collection<Long>
      addLinksNoTraits(sEntity, sRelation, new, instructions, invRef = id)
    } else {
      // supports new as Pair<Long, Traits> or Map<Long, Traits>
      addLinksWithTraits(sEntity, sRelation, new, instructions, invRef = id)
    }
    instructions.roots.addAll(children)

    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
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

      checkFieldConstraints(sEntity, field, sField, value)
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun checkEntityAndInsert(sEntity: SEntity, new: Any, instructions: Instructions, insOrUpd: InsertOrUpdate, include: Boolean = true) {
    // index before adding the instruction (direct references appear before this index)
    val index = if (instructions.size == 0) 0 else instructions.size - 1
    if (include)
      instructions.addInstruction(insOrUpd)

    // get input fields/relations (any values not part of the schema are ignored)
    val props = if (insOrUpd is Update) {
      (new as Map<String, Any?>).keys
    } else {
      // check if value is of correct type
      val vType = new.javaClass.kotlin.qualifiedName
      if (vType != sEntity.name)
        throw Exception("Invalid input type, expected ${sEntity.name} found ${vType}!")

      new.javaClass.kotlin.memberProperties.map { it.name }
    }

    // --------------------------------- check and process fields ---------------------------------
    for (prop in props) {
      val sField = sEntity.fields[prop] ?: continue // ignore fields that are not part of the schema

      // get field value if valid
      val fValue = if (insOrUpd is Update) {
        if (!sField.isInput)
          throw Exception("Invalid input field! - (${sEntity.name}, $prop)")

        if (new !is Map<*, *>)
          throw Exception("Expected Map of values for update! - (${sEntity.name}, $prop)")

        (new as Map<String, Any?>)[sField.name]
      } else {
        sField.getValue(new)
      }

      if (!sField.isOptional && fValue == null)
        throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

      // check field constraints if not null
      fValue?.let {
        if (insOrUpd is Update) {
          val type = it.javaClass.kotlin
          if (!TypeEngine.check(sField.type, type))
            throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $prop)")
        }

        checkFieldConstraints(sEntity, prop, sField, fValue)
      }

      // set even if null
      insOrUpd.putData(prop, fValue)
    }

    // --------------------------------- check and process relations ---------------------------------
    for (prop in props) {
      val sRelation = sEntity.rels[prop] ?: continue  // ignore relations that are not part of the schema

      // get relation value if valid
      val rValue = if (insOrUpd is Update) {
        if (!sRelation.isInput)
          throw Exception("Invalid input field! - (${sEntity.name}, $prop)")

        if (!sRelation.isOpen)
          throw Exception("Relation is not 'open'! - (${sEntity.name}, $prop)")

        if (new !is Map<*, *>)
          throw Exception("Expected Map of values for update! - (${sEntity.name}, $prop)")

        (new as Map<String, Any?>)[sRelation.name]
      } else {
        sRelation.getValue(new)
      }

      if (!sRelation.isOptional && rValue == null)
        throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

      // check relations constraints and create instruction (insert)
      when (sRelation.type) {
        RelationType.CREATE -> if (!sRelation.isCollection) {
          // TODO: should the ref-entity be deleted when null?
          if (rValue != null) setRelation(sEntity, sRelation, rValue, instructions, insOrUpd, index) else insOrUpd.putResolvedRef("ref_$prop", null)
        } else {
          addRelations(sEntity, sRelation, rValue!!, instructions, insOrUpd)
        }

        RelationType.LINK -> if (sRelation.traits.isEmpty()) {
          // links with no traits
          if (!sRelation.isCollection) {
            // TODO: should the link be deleted when null?
            if (rValue != null) setLinkNoTraits(sEntity, sRelation, rValue, instructions, insOrUpd) else insOrUpd.putResolvedRef("inv", null)
          } else {
            addLinksNoTraits(sEntity, sRelation, rValue!!, instructions, insOrUpd)
          }
        } else {
          // links with traits
          if (!sRelation.isCollection) {
            // TODO: should the link be deleted when null?
            if (rValue != null) setLinkWithTraits(sEntity, sRelation, rValue, instructions, insOrUpd) else insOrUpd.putResolvedRef("inv", null)
          } else {
            addLinksWithTraits(sEntity, sRelation, rValue!!, instructions, insOrUpd)
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

  private fun setLinkNoTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    tryCast<Long, Unit>(sEntity.name, sRelation.name, rValue) { link ->
      if (sEntity.type != EntityType.TRAIT) {
        val table = tableTranslator.getValue(sEntity.name)
        Insert("${table}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
          putResolvedRef("ref", link)
          putUnresolvedRef("inv", insOrUpd)
          instructions.addInstruction(this)
        }
      } else {
        insOrUpd.putResolvedRef("ref_${sRelation.name}", link)
      }
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun setLinkWithTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    tryCast<Pair<Long, Traits>, Unit>(sEntity.name, sRelation.name, rValue) { (link, traits) ->
      checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions).apply {
        putUnresolvedRef("inv", insOrUpd)
      }
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun addRelations(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): List<Insert> {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    return if (rValue !is Collection<*>) {
      listOf(addOneRelation(sEntity, sRelation, rValue, instructions, insOrUpd, invRef))
    } else {
      tryCast<Collection<Any>, List<Insert>>(sEntity.name, sRelation.name, rValue) { col ->
        col.map { addOneRelation(sEntity, sRelation, it, instructions, insOrUpd, invRef) }
      }
    }
  }

  private fun addOneRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): Insert {
    val invTable = tableTranslator.getValue(sEntity.name)
    val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name), AddAction(sEntity, sRelation)).apply {
      if (invRef == null) putUnresolvedRef("inv_${invTable}_${sRelation.name}", insOrUpd!!) else putResolvedRef("inv_${invTable}_${sRelation.name}", invRef)
    }

    checkEntityAndInsert(sRelation.ref, rValue, instructions, childInsert)
    return childInsert
  }

  @Suppress("UNCHECKED_CAST")
  private fun addLinksNoTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): List<Insert> {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // TODO: should traits support collections? (more tests required!) - change schema support

    return if (rValue !is Collection<*>) {
      tryCast<Long, List<Insert>>(sEntity.name, sRelation.name, rValue) { value ->
        listOf(addOneLinkNoTraits(sEntity, sRelation, value, instructions, insOrUpd, invRef))
      }
    } else {
      tryCast<Collection<Long>, List<Insert>>(sEntity.name, sRelation.name, rValue) { col ->
        col.map { addOneLinkNoTraits(sEntity, sRelation, it, instructions, insOrUpd, invRef) }
      }
    }
  }

  private fun addOneLinkNoTraits(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): Insert {
    val table = tableTranslator.getValue(sEntity.name)
    return Insert("${table}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
      putResolvedRef("ref", link)
      if (invRef == null) putUnresolvedRef("inv", insOrUpd!!) else putResolvedRef("inv", invRef)
      instructions.addInstruction(this)
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun addLinksWithTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): List<Insert> {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // TODO: should traits support collections? (more tests required!) - change schema support

    return if (rValue !is Map<*, *>) {
      tryCast<Pair<Long, Traits>, List<Insert>>(sEntity.name, sRelation.name, rValue) { (link, traits) ->
        listOf(addOneLinkWithTraits(sEntity, sRelation, link, traits, instructions, insOrUpd, invRef))
      }
    } else {
      tryCast<Map<Long, Traits>, List<Insert>>(sEntity.name, sRelation.name, rValue) { map ->
        map.map { (link, traits) -> addOneLinkWithTraits(sEntity, sRelation, link, traits, instructions, insOrUpd, invRef) }
      }
    }
  }

  private fun addOneLinkWithTraits(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Traits, instructions: Instructions, insOrUpd: InsertOrUpdate? = null, invRef: Long? = null): Insert {
    return checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions).apply {
      if (invRef == null) putUnresolvedRef("inv", insOrUpd!!) else putResolvedRef("inv", invRef)
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
private fun checkFieldConstraints(sEntity: SEntity, field: String, sField: SField, value: Any) {
  sField.checks.forEach {
    it.check(value)?.let { msg ->
      throw Exception("Constraint check failed '$msg'! - (${sEntity.name}, $field)")
    }
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