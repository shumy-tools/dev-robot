package dr.modification

import dr.DrServer
import dr.schema.*
import dr.schema.EventType.*
import dr.spi.IModificationAdaptor
import kotlin.reflect.full.memberProperties

/* ------------------------- api -------------------------*/
class ModificationEngine(private val adaptor: IModificationAdaptor) {
  private val schema: Schema by lazy { DrServer.schema }

  fun create(new: Any): Long {
    val vType = new.javaClass.kotlin
    val entity = vType.qualifiedName

    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - ($entity)")

    sEntity.onCreate(STARTED, null, new)

    val output = checkEntityAndMap(sEntity, new)
    println(output)

    val tx = adaptor.start()
    val id = try {
      sEntity.onCreate(CHECKED, null, new)
      tx.create(sEntity, new)
    } catch (ex: Exception) {
      tx.rollback()
      throw ex
    }

    tx.commit()
    sEntity.onCreate(COMMITED, id, new)

    // return derived values
    return id
  }

  fun update(entity: String, id: Long, data: Map<String, Any?>) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    sEntity.onUpdate(STARTED, id, data) // TODO: changed fields?

    for ((field, value) in data) {
      val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")
      checkFieldUpdate(sEntity, field, sField, value)
    }

    val tx = adaptor.start()
    try {
      sEntity.onUpdate(CHECKED, id, data)
      tx.update(sEntity, id, data)
    } catch (ex: Exception) {
      tx.rollback()
      throw ex
    }

    tx.commit()
    sEntity.onUpdate(COMMITED, id, data)
    // TODO: (return changed values)
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

    sEntity.onAdd(STARTED, id, sRelation, null, new)
    sEntity.onCreate(STARTED,null, new)

    checkEntityAndMap(sRelation.ref, new)

    val tx = adaptor.start()
    val link = try {
      sEntity.onCreate(CHECKED, null, new)
      sEntity.onAdd(CHECKED, id, sRelation, null, new)
      tx.add(sEntity, id, sRelation, new)
    } catch (ex: Exception) {
      tx.rollback()
      throw ex
    }

    tx.commit()
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

    sEntity.onLink(STARTED, id, sRelation, link)

      // TODO: check constraints

    val tx = adaptor.start()
    try {
      sEntity.onLink(CHECKED, id, sRelation, link)
      tx.link(sEntity, id, sRelation, link)
    } catch (ex: Exception) {
      tx.rollback()
      throw ex
    }

    tx.commit()
    sEntity.onLink(COMMITED, id, sRelation, link)
  }

  fun check(entity: String, field: String, value: Any?) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")
    checkFieldUpdate(sEntity, field, sField, value)
  }

  @Suppress("UNCHECKED_CAST")
  private fun checkEntityAndMap(sEntity: SEntity, new: Any): Map<String, Any> {
    val vType = new.javaClass.kotlin
    val entity = vType.qualifiedName

    // TODO: list of create operations for the relational db instead of map?
    val output = mutableMapOf<String, Any>()

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
            output[prop] = fValue
          }
        }

        sRelation != null -> {
          val rValue = sRelation.getValue(new)!!
          when (sRelation.type) {
            RelationType.CREATE -> if (!sRelation.isCollection) {
              output[prop] = checkEntityAndMap(sRelation.ref, rValue)
            } else {
              val col = (rValue as Collection<Any>)
              output[prop] = col.map { checkEntityAndMap(sRelation.ref, it) }
            }

            RelationType.LINK -> if (sRelation.traits.isEmpty()) {
              output[prop] = rValue // Long or Collection<Long>
            } else {
              if (!sRelation.isCollection) {
                val (id, traits) = rValue as Pair<Long, Traits>
                output[prop] = Pair(id, checkTraitsAndMap(sEntity, sRelation, traits))
              } else {
                val map = rValue as Map<Long, Traits>
                output[prop] = map.map { (id, traits) -> Pair(id, checkTraitsAndMap(sEntity, sRelation, traits)) }
              }
            }
          }
        }

        else -> throw Exception("Entity field/relation not found! - ($entity, $prop)")
      }
    }

    return output
  }

  private fun checkTraitsAndMap(sEntity: SEntity, sRelation: SRelation, traits: Traits): Map<String, Any> {
    val tMap = traits.traits.map {
      val name = it.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      if (!sRelation.traits.contains(sTrait))
        throw Exception("Trait '${sTrait.name}' not part of the model! - (${sEntity.name}, ${sRelation.name})")

      sTrait to checkEntityAndMap(sTrait, it)
    }.toMap()

    for (trait in sRelation.traits) {
      if (!tMap.containsKey(trait))
        throw Exception("Trait '${trait.name}' not present in data! - (${sEntity.name}, ${sRelation.name})")
    }

    return tMap.mapKeys { (sTrait, _) -> sTrait.name }
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