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

  fun check(sEntity: SEntity, json: String): Map<String, List<String>> {
    val entity = processor.update(sEntity, json)
    return entity.checkFields()
  }
}