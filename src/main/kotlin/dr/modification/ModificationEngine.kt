package dr.modification

import dr.DrServer
import dr.schema.*
import dr.schema.EventType.*
import dr.spi.IModificationAdaptor
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

/* ------------------------- api -------------------------*/
class ModificationEngine(private val adaptor: IModificationAdaptor) {
  private val schema: Schema by lazy { DrServer.schema }

  fun create(new: Any): Pair<Long, Map<String, Any?>> {
    val vType = new.javaClass.kotlin
    val entity = vType.qualifiedName

    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - ($entity)")

    sEntity.onCreate(STARTED, null, new)
    checkEntity(sEntity, new)

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
    return Pair(id, derived(sEntity, new))
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
    checkEntity(sRelation.ref, new)

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
}

/* ------------------------- helpers -------------------------*/
@Suppress("UNCHECKED_CAST")
private fun derived(sEntity: SEntity, new: Any): Map<String, Any?> {
  val derived = mutableMapOf<String, Any?>()
  // TODO: get derived tree

  val vType = new.javaClass.kotlin
  for (param in vType.memberProperties) {
    val prop = param.name
    val sField = sEntity.fields[prop]
    val sRelation = sEntity.rels[prop]
    when {
      sField != null -> if (!sField.isInput) {
        derived[prop] = sField.getValue(new)
      }

      sRelation != null -> if (!sRelation.isInput) {
        val rValue = sRelation.getValue(new)
        rValue?.let {
          if (sRelation.type == RelationType.CREATE) {
            if (!sRelation.isCollection) {
              derived[prop] = derived(sRelation.ref, it)
            } else {
              derived[prop] = (rValue as Collection<Any?>).map { item ->
                if (item == null)
                  throw Exception("Invalid 'null' derived on map-item! - (${sEntity.name}, $prop)")

                // TODO: how to set id
                0L to derived(sRelation.ref, item)
              }.toMap()
            }
          } else {
            if (!sRelation.isCollection) {
              derived[prop] = rValue as Long
            } else {
              derived[prop] = (rValue as Collection<Long?>).map { key ->
                if (key == null)
                  throw Exception("Invalid 'null' derived on collection-item! - (${sEntity.name}, $prop)")
                key
              }
            }
          }
        }
      }

      else -> throw Exception("Entity field/relation not found! - (${sEntity.name}, $prop)")
    }
  }

  return derived
}

@Suppress("UNCHECKED_CAST")
private fun checkEntity(sEntity: SEntity, new: Any) {
  val vType = new.javaClass.kotlin
  val entity = vType.qualifiedName

  // TODO: check if entity exist? (by id)

  // only check inputs (no need for 'if (!sRelation.isInput)' )
  for (param in vType.primaryConstructor!!.parameters) {
    val prop = param.name!!
    val sField = sEntity.fields[prop]
    val sRelation = sEntity.rels[prop]
    when {
      sField != null -> {
        val fValue = sField.getValue(new)
        if (!sField.isOptional && fValue == null)
          throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

        fValue?.let { checkFieldConstraints(sEntity, prop, sField, fValue) }
      }

      sRelation != null -> {
        val rValue = sRelation.getValue(new)
        if (!sRelation.isOptional && rValue == null)
          throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

        if (sRelation.type == RelationType.CREATE) {
          if (!sRelation.isCollection) {
            rValue?.let { checkEntity(sRelation.ref, it) }
          } else {
            if (rValue == null)
              throw Exception("Invalid 'null' input! - (${sEntity.name}, $prop)")

            (rValue as Collection<Any?>).forEach { item ->
              if (item == null)
                throw Exception("Invalid 'null' input on map-item! - (${sEntity.name}, $prop)")
              checkEntity(sRelation.ref, item)
            }
          }
        } else {
          // TODO: check if links exist?  (by id)
        }
      }

      else -> throw Exception("Entity field/relation not found! - ($entity, $prop)")
    }
  }
}

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