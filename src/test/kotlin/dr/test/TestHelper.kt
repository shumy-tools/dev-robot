package dr.test

import dr.modification.ModificationEngine
import dr.schema.ActionType
import dr.schema.SParser
import dr.schema.Schema
import dr.spi.*
import kotlin.reflect.KClass

object TestHelper {
  fun modification(inSchema: Schema, callback: (Instructions) -> Unit): ModificationEngine {
    return ModificationEngine(TestModificationAdaptor(callback)).apply {
      schema = inSchema
      tableTranslator = schema.tables()
    }
  }
}

fun Instruction.isCreate(clazz: KClass<out Any>, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  assert(insert.action.type == ActionType.CREATE)
  assert(insert.action.sEntity.name == clazz.qualifiedName)
  assert(insert.table == table)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUpdate(clazz: KClass<out Any>, id: Long, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Update
  assert(update.action.type == ActionType.UPDATE)
  assert(update.id == id)
  assert(update.action.sEntity.name == clazz.qualifiedName)
  assert(update.table == table)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isAdd(clazz: KClass<out Any>, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Insert
  assert(update.action.type == ActionType.ADD)
  assert(update.action.sEntity.name == clazz.qualifiedName)
  assert(update.table == table)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isLink(clazz: KClass<out Any>, relation: String, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  val lAction = insert.action as LinkAction
  assert(insert.action.type == ActionType.LINK)
  assert(lAction.sEntity.name == clazz.qualifiedName)
  assert(lAction.sRelation.name == relation)
  assert(insert.table == table)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUnlink(clazz: KClass<out Any>, relation: String, table: String, link: Long) {
  val delete = this as Delete
  val lAction = delete.action as UnlinkAction
  assert(delete.action.type == ActionType.UNLINK)
  assert(lAction.sEntity.name == clazz.qualifiedName)
  assert(lAction.sRelation.name == relation)
  assert(delete.table == table)
  assert(delete.id == link)
}

private class TestModificationAdaptor(val callback: (Instructions) -> Unit): IModificationAdaptor {
  private var idSeq = 9L;

  override fun commit(instructions: Instructions): List<Long> {
    val ids =  instructions.exec {
      when (it) {
        is Insert -> ++idSeq
        is Update -> it.id
        is Delete -> it.id
      }
    }

    callback(instructions)
    return ids
  }
}

private fun Schema.tables(): Map<String, String> {
  return this.entities.map { (name, _) ->
    name to name.replace('.', '_').toLowerCase()
  }.toMap()
}