package dr.modification

import dr.DrServer
import dr.schema.*
import dr.schema.EventType.*
import dr.spi.*
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.typeOf

/* ------------------------- api -------------------------*/
class ModificationEngine(private val adaptor: IModificationAdaptor) {
  private val schema: Schema by lazy { DrServer.schema }

  private val tableTranslator = schema.entities.map { (name, _) ->
    name to name.replace('.', '_').toLowerCase()
  }.toMap()

  fun create(new: Any): Long {
    val entity = new.javaClass.kotlin.qualifiedName
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - ($entity)")

    // create entity
    val insert = Insert(tableTranslator.getValue(sEntity.name))
    val instructions = Instructions(insert)
    checkEntityAndInsert(sEntity, null, new, instructions, insert)

    // commit instructions
    val id = adaptor.commit(instructions)
    sEntity.onCreate(COMMITED, id, new)

    return id
  }

  fun update(entity: String, id: Long, new: Map<String, Any?>) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")

    // update entity
    val update = Update(sEntity.name, id)
    val instructions = Instructions(update)
    checkEntityAndInsert(sEntity, id, new, instructions, update)

    // commit instructions
    adaptor.commit(instructions)
    sEntity.onUpdate(COMMITED, id, new)
  }

  fun add(entity: String, id: Long, rel: String, new: Any): Long {
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

    //TODO: add support for many?

    // creates one child entity
    val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
    val instructions = Instructions(childInsert)
    checkEntityAndInsert(sRelation.ref, null, new, instructions, childInsert)

    // add inv relation (already resolved)
    val table = tableTranslator.getValue(sEntity.name)
    childInsert.putData("inv_${table}_${sRelation.name}", id)
    sEntity.onAdd(CHECKED, id, sRelation, null, new)

    // commit instructions
    val link = adaptor.commit(instructions)
    sEntity.onCreate(COMMITED, link, new)
    sEntity.onAdd(COMMITED, id, sRelation, link, new)

    return link
  }

  fun link(entity: String, id: Long, rel: String, new: Any) {
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
    val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
    val instructions = Instructions(childInsert)
    if (sRelation.traits.isEmpty()) {
      // supports new as Long or Collection<Long>
      addLinksNoTraits(sEntity, sRelation, new, instructions, childInsert)
    } else {
      // supports new as Pair<Long, Traits> or Map<Long, Traits>
      addLinksWithTraits(sEntity, sRelation, new, instructions, childInsert)
    }

    // commit instructions
    adaptor.commit(instructions)
    sEntity.onLink(COMMITED, id, sRelation, new)
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
  private fun checkEntityAndInsert(sEntity: SEntity, id: Long?, new: Any, instructions: Instructions, insOrUpd: InsertOrUpdate, include: Boolean = true) {
    val instData = insOrUpd.data as LinkedHashMap<String, Any?>

    // index before adding the instruction (direct references appear before this index)
    val index = if (instructions.list.size == 0) 0 else instructions.list.size - 1
    if (include)
      instructions.list.add(insOrUpd)

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
      instData[prop] = fValue
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
          if (rValue != null) setRelation(sEntity, sRelation, rValue, instructions, insOrUpd, index) else instData["ref_$prop"] = null
        } else {
          addRelations(sEntity, sRelation, rValue!!, instructions, insOrUpd)
        }

        RelationType.LINK -> if (sRelation.traits.isEmpty()) {
          // links with no traits
          if (!sRelation.isCollection) {
            if (rValue != null) setLinkNoTraits(sEntity, sRelation, rValue, instructions, insOrUpd) else instData["inv"] = null
          } else {
            addLinksNoTraits(sEntity, sRelation, rValue!!, instructions, insOrUpd)
          }
        } else {
          // links with traits
          if (!sRelation.isCollection) {
            if (rValue != null) setLinkWithTraits(sEntity, sRelation, rValue, instructions, insOrUpd) else instData["inv"] = null
          } else {
            addLinksWithTraits(sEntity, sRelation, rValue!!, instructions, insOrUpd)
          }
        }
      }
    }

    when (insOrUpd) {
      is Insert -> sEntity.onCreate(CHECKED, null, new)
      is Update -> sEntity.onUpdate(CHECKED, id!!, new as Map<String, Any?>) // TODO: changed fields?
    }
  }

  private fun setRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate, index: Int) {
    if (insOrUpd is Update) {
      val type = rValue.javaClass.kotlin.qualifiedName
      if (type != sRelation.ref.name)
        throw Exception("Invalid field type, expected ${sRelation.ref.name} found ${type}! - (${sEntity.name}, ${sRelation.name})")
    }

    if (sEntity.type != EntityType.TRAIT) {
      val refInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
      checkEntityAndInsert(sRelation.ref, null, rValue, instructions, refInsert, false)
      insOrUpd.nativeRefs["ref_${sRelation.name}"] = refInsert

      // direct references must be inserted before the parent insert
      instructions.list.add(index, refInsert)
    } else {
      // TODO: experimental embedded traits
      checkEntityAndInsert(sRelation.ref, null, rValue, instructions, insOrUpd, false)
    }
  }

  private fun setLinkNoTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    try {
      val link = rValue as Long
      if (sEntity.type != EntityType.TRAIT) {
        /* experimental embedded links!
        val linkInsert = insertLink(sEntity, sRelation, link, instructions, false)
        insOrUpd.nativeRefs["ref_$prop"] = linkInsert

        // direct references must be inserted before the parent insert
        instructions.list.add(index, linkInsert)
        */

        val table = tableTranslator.getValue(sEntity.name)
        val linkInsert = insertLink(table, sRelation, link, instructions)
        linkInsert.nativeRefs["inv"] = insOrUpd
      } else {
        // TODO: experimental embedded traits
        insOrUpd.putData("ref_${sRelation.name}", link)
      }
    } catch (ex: ClassCastException) {
      throw Exception("Invalid field type, expected Long found ${rValue.javaClass.kotlin.qualifiedName}! - (${sEntity.name}, ${sRelation.name})")
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun setLinkWithTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    try {
      val (link, traits) = rValue as Pair<Long, Traits>
      val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
      linkInsert.nativeRefs["inv"] = insOrUpd
      sEntity.onLink(CHECKED, null, sRelation, link)
    } catch (ex: ClassCastException) {
      throw Exception("Invalid field type, expected Pair<Long, Traits> found ${rValue.javaClass.kotlin.qualifiedName}! - (${sEntity.name}, ${sRelation.name})")
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun addRelations(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    if (rValue !is Collection<*>) {
      addOneRelation(sEntity, sRelation, rValue, instructions, insOrUpd)
    } else {
      val col = (rValue as Collection<Any>)
      col.forEach { addOneRelation(sEntity, sRelation, it, instructions, insOrUpd) }
    }
  }

  private fun addOneRelation(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
    checkEntityAndInsert(sRelation.ref, null, rValue, instructions, childInsert)
    val table = tableTranslator.getValue(sEntity.name)
    childInsert.nativeRefs["inv_${table}_${sRelation.name}"] = insOrUpd
    sEntity.onAdd(CHECKED, null, sRelation, null,  rValue)
  }

  @Suppress("UNCHECKED_CAST")
  private fun addLinksNoTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // TODO: should traits support collections? (more tests required!) - change schema support

    if (rValue !is Collection<*>) {
      addOneLinkNoTraits(sEntity, sRelation, rValue as Long, instructions, insOrUpd)
    } else {
      val col = (rValue as Collection<Long>)
      col.forEach { addOneLinkNoTraits(sEntity, sRelation, it, instructions, insOrUpd) }
    }
  }

  private fun addOneLinkNoTraits(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    val table = tableTranslator.getValue(sEntity.name)
    val linkInsert = insertLink(table, sRelation, link, instructions)
    linkInsert.nativeRefs["inv"] = insOrUpd
    sEntity.onLink(CHECKED, null, sRelation, link)
  }

  @Suppress("UNCHECKED_CAST")
  private fun addLinksWithTraits(sEntity: SEntity, sRelation: SRelation, rValue: Any, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    if (insOrUpd is Update)
      throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, ${sRelation.name})")

    // TODO: should traits support collections? (more tests required!) - change schema support

    if (rValue !is Map<*, *>) {
      val (link, traits) = rValue as Pair<Long, Traits>
      addOneLinkWithTraits(sEntity, sRelation, link, traits, instructions, insOrUpd)
    } else {
      val map = rValue as Map<Long, Traits>
      map.forEach { (link, traits) -> addOneLinkWithTraits(sEntity, sRelation, link, traits, instructions, insOrUpd) }
    }
  }

  private fun addOneLinkWithTraits(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Traits, instructions: Instructions, insOrUpd: InsertOrUpdate) {
    val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
    linkInsert.nativeRefs["inv"] = insOrUpd
    sEntity.onLink(CHECKED, null, sRelation, link)
  }

  private fun checkTraitsAndInsert(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Traits, instructions: Instructions): Insert {
    if (sRelation.traits.size != traits.traits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    val table = tableTranslator.getValue(sEntity.name)
    val traitInsert = Insert("${table}__${sRelation.name}")
    traitInsert.putData("ref", link)
    instructions.list.add(traitInsert)

    val processedTraits = mutableSetOf<Any>()
    for (trait in traits.traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      processedTraits.add(sTrait)
      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      // Does not insert. Compact all trait fields
      checkEntityAndInsert(sTrait, null, trait, instructions, traitInsert, false)
    }

    if (sRelation.traits.size != processedTraits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    return traitInsert
  }
}


/* ------------------------- helpers -------------------------*/
private fun insertLink(table: String, sRelation: SRelation, link: Long, instructions: Instructions, include: Boolean = true): Insert {
  val linkInsert = Insert("${table}__${sRelation.name}")
  linkInsert.putData("ref", link)

  if (include)
    instructions.list.add(linkInsert)

  return linkInsert
}

private fun checkFieldConstraints(sEntity: SEntity, field: String, sField: SField, value: Any) {
  sField.checks.forEach {
    it.check(value)?.let { msg ->
      throw Exception("Constraint check failed '$msg'! - (${sEntity.name}, $field)")
    }
  }
}