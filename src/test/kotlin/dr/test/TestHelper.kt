package dr.test

import dr.io.*
import dr.schema.ActionType
import kotlin.reflect.KClass


fun Instruction.isCreate(clazz: KClass<out Any>, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  assert(insert.action.type == ActionType.CREATE)
  assert(insert.action.entity.name == clazz.qualifiedName)
  assert(insert.table == table)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUpdate(clazz: KClass<out Any>, id: Long, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Update
  assert(update.action.type == ActionType.UPDATE)
  assert(update.id == id)
  assert(update.action.entity.name == clazz.qualifiedName)
  assert(update.table == table)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isAdd(clazz: KClass<out Any>, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Insert
  assert(update.action.type == ActionType.ADD)
  assert(update.action.entity.name == clazz.qualifiedName)
  assert(update.table == table)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isLink(clazz: KClass<out Any>, relation: String, table: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  val lAction = insert.action as LinkAction
  assert(insert.action.type == ActionType.LINK)
  assert(lAction.entity.name == clazz.qualifiedName)
  assert(lAction.sRelation.name == relation)
  assert(insert.table == table)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUnlink(clazz: KClass<out Any>, relation: String, table: String, refs: Map<String, Any> = emptyMap()) {
  val delete = this as Delete
  val lAction = delete.action as UnlinkAction
  assert(delete.action.type == ActionType.UNLINK)
  assert(lAction.entity.name == clazz.qualifiedName)
  assert(lAction.sRelation.name == relation)
  assert(delete.table == table)
  assert(delete.resolvedRefs == refs)
}