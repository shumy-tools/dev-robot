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
    val vType = new.javaClass.kotlin
    val entity = vType.qualifiedName

    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - ($entity)")

    val insert = Insert(tableTranslator.getValue(sEntity.name))
    val instructions = Instructions(insert)
    checkEntityAndInsert(sEntity, null, new, instructions, insert)

    val id = adaptor.commit(instructions)
    sEntity.onCreate(COMMITED, id, new)

    return id
  }

  fun update(entity: String, id: Long, new: Map<String, Any?>) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")

    val update = Update(sEntity.name, id)
    val instructions = Instructions(update)
    checkEntityAndInsert(sEntity, id, new, instructions, update)

    adaptor.commit(instructions)
    sEntity.onUpdate(COMMITED, id, new)
  }

  fun add(entity: String, id: Long, rel: String, new: Any): Long {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    if (!sRelation.isInput)
      throw Exception("Invalid input relation! - (${sEntity.name}, $rel)")

    val vType = new.javaClass.kotlin
    val vRelation = vType.qualifiedName
    val vEntity = schema.entities[vRelation] ?: throw Exception("Entity type not found! - ($vRelation)")
    if (vEntity != sRelation.ref)
      throw Exception("Invalid input type, expected ${sRelation.ref.name} found ${vEntity.name}!")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - ($entity, $rel)")

    if (sRelation.type != RelationType.CREATE)
      throw Exception("Relation is not 'create'! - ($entity, $rel)")

    val insert = Insert(tableTranslator.getValue(sRelation.ref.name))
    val instructions = Instructions(insert)
    checkEntityAndInsert(sRelation.ref, null, new, instructions, insert)
    sEntity.onAdd(CHECKED, id, sRelation, null, new)

    val link = adaptor.commit(instructions)
    sEntity.onCreate(COMMITED, link, new)
    sEntity.onAdd(COMMITED, id, sRelation, link, new)

    return link
  }

  fun link(entity: String, id: Long, rel: String, link: Long) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    if (!sRelation.isOpen)
      throw Exception("Relation is not 'open'! - ($entity, $rel)")

    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    val insert = Insert(tableTranslator.getValue(sRelation.ref.name))
    val instructions = Instructions(insert)
    // TODO: check constraints

    adaptor.commit(instructions)
    sEntity.onLink(COMMITED, id, sRelation, link)
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
      new.javaClass.kotlin.memberProperties.map { it.name }
    }

    // check and process fields
    for (prop in props) {
      val sField = sEntity.fields[prop] ?: continue
      val fValue = sField.getValue(new)

      if (insOrUpd is Update && !sField.isInput)
        throw Exception("Invalid input field! - (${sEntity.name}, $prop)")

      if (!sField.isOptional && fValue == null)
        throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

      fValue?.let {
        if (insOrUpd is Update) {
          val type = it.javaClass.kotlin
          if (!TypeEngine.check(sField.type, type))
            throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $prop)")
        }

        checkFieldConstraints(sEntity, prop, sField, fValue)
        instData[prop] = fValue
      }
    }

    // check and process relations
    for (prop in props) {
      val sRelation = sEntity.rels[prop] ?: continue
      val rValue = sRelation.getValue(new)

      if (insOrUpd is Update) {
        if (!sRelation.isInput)
          throw Exception("Invalid input field! - (${sEntity.name}, $prop)")

        if (!sRelation.isOpen)
          throw Exception("Relation is not 'open'! - (${sEntity.name}, $prop)")
      }

      if (!sRelation.isOptional && rValue == null)
        throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

      when (sRelation.type) {
        RelationType.CREATE -> if (!sRelation.isCollection) {
          if (rValue != null) {
            if (insOrUpd is Update) {
              val type = rValue.javaClass.kotlin.qualifiedName
              if (type != sRelation.ref.name)
                throw Exception("Invalid field type, expected ${sRelation.ref.name} found ${type}! - (${sEntity.name}, $prop)")
            }

            if (sEntity.type != EntityType.TRAIT) {
              val refInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
              checkEntityAndInsert(sRelation.ref, null, rValue, instructions, refInsert, false)
              insOrUpd.nativeRefs["ref_$prop"] = refInsert

              // direct references must be inserted before the parent insert
              instructions.list.add(index, refInsert)
            } else {
              // TODO: experimental embedded traits
              checkEntityAndInsert(sRelation.ref, null, rValue, instructions, insOrUpd, false)
            }
          } else {
            instData["ref_$prop"] = null
          }
        } else {
          if (insOrUpd is Update)
            throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, $prop)")

          val col = (rValue as Collection<Any>)
          col.forEach {
            val childInsert = Insert(tableTranslator.getValue(sRelation.ref.name))
            checkEntityAndInsert(sRelation.ref, null, it, instructions, childInsert)
            val table = tableTranslator.getValue(sEntity.name)
            childInsert.nativeRefs["inv_${table}_$prop"] = insOrUpd
            sEntity.onAdd(CHECKED, null, sRelation, null,  rValue)
          }
        }

        RelationType.LINK -> if (sRelation.traits.isEmpty()) {
          if (!sRelation.isCollection) {
            if (rValue != null) {
              try {
                  val link = rValue as Long
                  if (sEntity.type != EntityType.TRAIT) {
                    /* experimental embedded links!
                    val linkInsert = insertLink(sEntity, sRelation, link, instructions, false)
                    insOrUpd.nativeRefs["ref_$prop"] = linkInsert

                    // direct references must be inserted before the parent insert
                    instructions.list.add(index, linkInsert)
                    */

                    val linkInsert = insertLink(sEntity, sRelation, link, instructions)
                    linkInsert.nativeRefs["inv"] = insOrUpd
                  } else {
                    // TODO: experimental embedded traits
                    insOrUpd.putData("ref_$prop", link)
                  }
              } catch (ex: ClassCastException) {
                throw Exception("Invalid field type, expected Long found ${rValue.javaClass.kotlin.qualifiedName}! - (${sEntity.name}, $prop)")
              }
            } else {
              instData["inv"] = null
            }
          } else {
            if (insOrUpd is Update)
              throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, $prop)")

            // TODO: should traits support collections? (more tests required!) - change schema support

            val list = rValue as Collection<Long>
            list.forEach { link ->
              val linkInsert = insertLink(sEntity, sRelation, link, instructions)
              linkInsert.nativeRefs["inv"] = insOrUpd
              sEntity.onLink(CHECKED, null, sRelation, link)
            }
          }
        } else {
          if (!sRelation.isCollection) {
            if (rValue != null) {
              try {
                val (link, traits) = rValue as Pair<Long, Traits>
                val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
                linkInsert.nativeRefs["inv"] = insOrUpd
                sEntity.onLink(CHECKED, null, sRelation, link)
              } catch (ex: ClassCastException) {
                throw Exception("Invalid field type, expected Pair<Long, Traits> found ${rValue.javaClass.kotlin.qualifiedName}! - (${sEntity.name}, $prop)")
              }
            } else {
              instData["inv"] = null
            }
          } else {
            if (insOrUpd is Update)
              throw Exception("Cannot update collections, use 'add/link' instead! - (${sEntity.name}, $prop)")

            // TODO: should traits support collections? (more tests required!) - change schema support

            val map = rValue as Map<Long, Traits>
            map.forEach { (link, traits) ->
              val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
              linkInsert.nativeRefs["inv"] = insOrUpd
              sEntity.onLink(CHECKED, null, sRelation, link)
            }
          }
        }
      }
    }

    when (insOrUpd) {
      is Insert -> sEntity.onCreate(CHECKED, null, new)
      is Update -> sEntity.onUpdate(CHECKED, id!!, new as Map<String, Any?>) // TODO: changed fields?
    }
  }

  private fun insertLink(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions, include: Boolean = true): Insert {
    val table = tableTranslator.getValue(sEntity.name)
    val linkInsert = Insert("${table}__${sRelation.name}")
    linkInsert.putData("ref", link)

    if (include)
      instructions.list.add(linkInsert)

    return linkInsert
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
private fun checkFieldConstraints(sEntity: SEntity, field: String, sField: SField, value: Any) {
  sField.checks.forEach {
    it.check(value)?.let { msg ->
      throw Exception("Constraint check failed '$msg'! - (${sEntity.name}, $field)")
    }
  }
}