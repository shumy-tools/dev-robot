package dr.engine

import dr.DrServer
import dr.io.DEntityTranslator
import dr.io.InputProcessor
import dr.schema.*
import dr.spi.IAuthorizer
import dr.spi.IModificationAdaptor

/* ------------------------- api -------------------------*/
class ModificationEngine(
  private val processor: InputProcessor,
  private val translator: DEntityTranslator,
  private val adaptor: IModificationAdaptor,
  private val authorizer: IAuthorizer
) {

  fun create(sEntity: SEntity, json: String): Map<String, Any?> {
    val entity = processor.create(sEntity, json)
    val instructions = translator.create(entity)

    adaptor.commit(instructions)
    return instructions.output
  }

  fun update(sEntity: SEntity, id: Long, json: String): Map<String, Any?> {
    val entity = processor.update(sEntity, json)
    val instructions = translator.update(id, entity)

    adaptor.commit(instructions)
    return instructions.output
  }

  /*

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
  }*/
}