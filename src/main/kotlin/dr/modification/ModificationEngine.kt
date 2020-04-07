package dr.modification

import dr.DrServer
import dr.schema.*
import dr.schema.EventType.*
import dr.spi.*
import kotlin.reflect.full.memberProperties

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

    val instructions = Instructions()
    checkEntityAndInsert(sEntity, new, instructions)

    val id = adaptor.commit(instructions)
    sEntity.onCreate(COMMITED, id, new)

    // TODO: return derived values?
    return id
  }

  fun update(entity: String, id: Long, data: Map<String, Any?>) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")

    val instructions = Instructions()
    val uData = linkedMapOf<String, Any?>()
    val update = Update(sEntity.name, id, uData)
    instructions.list.add(update)

    for ((field, value) in data) {
      // TODO: check references
      val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")
      checkFieldUpdate(sEntity, field, sField, value)
      uData[field] = value
    }
    sEntity.onUpdate(CHECKED, id, data) // TODO: changed fields?

    adaptor.commit(instructions)
    sEntity.onUpdate(COMMITED, id, data)

    // TODO: return changed values?
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

    val instructions = Instructions()
    checkEntityAndInsert(sRelation.ref, new, instructions)
    sEntity.onAdd(CHECKED, id, sRelation, null, new)

    val link = adaptor.commit(instructions)
    //sEntity.onCreate(COMMITED, link, new)
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

    // TODO: check constraints

    adaptor.commit(Instructions())
    sEntity.onLink(COMMITED, id, sRelation, link)
  }

  fun check(entity: String, field: String, value: Any?) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")
    checkFieldUpdate(sEntity, field, sField, value)
  }

  @Suppress("UNCHECKED_CAST")
  private fun checkEntityAndInsert(sEntity: SEntity, new: Any, instructions: Instructions, include: Boolean = true): Insert {
    val vType = new.javaClass.kotlin
    val entity = vType.qualifiedName

    val data = linkedMapOf<String, Any?>()
    val insert = Insert(tableTranslator.getValue(sEntity.name), data)

    if (include)
      instructions.list.add(insert)

    for (param in vType.memberProperties) {
      val prop = param.name
      val sField = sEntity.fields[prop]
      val sRelation = sEntity.rels[prop]
      when {
        sField != null -> {
          val fValue = sField.getValue(new)
          fValue?.let {
            if (sField.isInput)
              checkFieldConstraints(sEntity, prop, sField, fValue)
            data[prop] = fValue
          }
        }

        sRelation != null -> {
          val rValue = sRelation.getValue(new)!!
          when (sRelation.type) {
            RelationType.CREATE -> if (!sRelation.isCollection) {
              val childInsert = checkEntityAndInsert(sRelation.ref, rValue, instructions)
              insert.nativeRefs["ref_$prop"] = childInsert                        // direct reference
              sEntity.onAdd(CHECKED, null, sRelation, null,  rValue)
            } else {
              val col = (rValue as Collection<Any>)
              col.forEach {
                val childInsert = checkEntityAndInsert(sRelation.ref, it, instructions)
                val table = tableTranslator.getValue(sEntity.name)
                childInsert.nativeRefs["inv_ref_${table}_$prop"] = insert             // inverted reference
                sEntity.onAdd(CHECKED, null, sRelation, null,  rValue)
              }
            }

            RelationType.LINK -> if (sRelation.traits.isEmpty()) {
              if (!sRelation.isCollection) {
                val link = rValue as Long
                val linkInsert = insertLink(sEntity, sRelation, link, instructions)
                linkInsert.nativeRefs["inv_ref"] = insert                         // inverted reference
                sEntity.onLink(CHECKED, null, sRelation, link)
              } else {
                val list = rValue as Collection<Long>
                list.forEach { link ->
                  val linkInsert = insertLink(sEntity, sRelation, link, instructions)
                  linkInsert.nativeRefs["inv_ref"] = insert                       // inverted reference
                  sEntity.onLink(CHECKED, null, sRelation, link)
                }
              }
            } else {
              if (!sRelation.isCollection) {
                val (link, traits) = rValue as Pair<Long, Traits>
                val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
                linkInsert.nativeRefs["inv_ref"] = insert                         // inverted reference
                sEntity.onLink(CHECKED, null, sRelation, link)
              } else {
                val map = rValue as Map<Long, Traits>
                map.forEach { (link, traits) ->
                  val linkInsert = checkTraitsAndInsert(sEntity, sRelation, link, traits, instructions)
                  linkInsert.nativeRefs["inv_ref"] = insert                       // inverted reference
                  sEntity.onLink(CHECKED, null, sRelation, link)
                }
              }
            }
          }
        }

        else -> throw Exception("Entity field/relation not found! - ($entity, $prop)")
      }
    }

    sEntity.onCreate(CHECKED, null, new)
    return insert
  }

  private fun insertLink(sEntity: SEntity, sRelation: SRelation, link: Long, instructions: Instructions): Insert {
    val table = tableTranslator.getValue(sEntity.name)
    val linkInsert = Insert("${table}_${sRelation.name}", linkedMapOf("ref" to link))
    instructions.list.add(linkInsert)
    return linkInsert
  }

  private fun checkTraitsAndInsert(sEntity: SEntity, sRelation: SRelation, link: Long, traits: Traits, instructions: Instructions): Insert {
    if (sRelation.traits.size != traits.traits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    val table = tableTranslator.getValue(sEntity.name)
    val linkData: MutableMap<String, Any?> = linkedMapOf("ref" to link)
    val linkInsert = Insert("${table}_${sRelation.name}", linkData)
    instructions.list.add(linkInsert)

    val processedTraits = mutableSetOf<Any>()
    for (trait in traits.traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      processedTraits.add(sTrait)
      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      // Does not insert. Compact all traits fields
      val childInsert = checkEntityAndInsert(sTrait, trait, instructions, false)
      linkData.putAll(childInsert.data)

      sTrait to childInsert
    }

    if (sRelation.traits.size != processedTraits.size)
      throw Exception("Invalid number of traits, expected '${sRelation.traits.size}' found '${traits.traits.size}'! - (${sEntity.name}, ${sRelation.name})")

    return linkInsert
  }
}

/* ------------------------- helpers -------------------------*/
private fun checkFieldUpdate(sEntity: SEntity, field: String, sField: SField, value: Any?) {
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

private fun checkFieldConstraints(sEntity: SEntity, field: String, sField: SField, value: Any) {
  sField.checks.forEach {
    it.check(value)?.let { msg ->
      throw Exception("Constraint check failed '$msg'! - (${sEntity.name}, $field)")
    }
  }
}