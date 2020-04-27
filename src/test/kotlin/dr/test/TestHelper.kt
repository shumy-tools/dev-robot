package dr.test

import dr.io.*
import kotlin.reflect.KClass
import dr.schema.ActionType.*


fun Instruction.isCreate(clazz: KClass<out Any>, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  assert(insert.action == CREATE)
  assert(insert.table.sEntity.name == clazz.qualifiedName)
  assert(insert.table.sRelation == null)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUpdate(clazz: KClass<out Any>, id: Long, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Update
  assert(update.action == UPDATE)
  assert(update.id == id)
  assert(update.table.sEntity.name == clazz.qualifiedName)
  assert(update.table.sRelation == null)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isAdd(clazz: KClass<out Any>, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val update = this as Insert
  assert(update.action == ADD)
  assert(update.table.sEntity.name == clazz.qualifiedName)
  assert(update.table.sRelation == null)
  assert(update.data == data)
  assert(update.resolvedRefs == refs)
}

fun Instruction.isLink(clazz: KClass<out Any>, relation: String, data: Map<String, Any>, refs: Map<String, Any> = emptyMap()) {
  val insert = this as Insert
  assert(insert.action == LINK)
  assert(insert.table.sEntity.name == clazz.qualifiedName)
  assert(insert.table.sRelation?.name == relation)
  assert(insert.data == data)
  assert(insert.resolvedRefs == refs)
}

fun Instruction.isUnlink(clazz: KClass<out Any>, relation: String, refs: Map<String, Any> = emptyMap()) {
  val delete = this as Delete
  assert(delete.action == UNLINK)
  assert(delete.table.sEntity.name == clazz.qualifiedName)
  assert(delete.table.sRelation?.name == relation)
  assert(delete.resolvedRefs == refs)
}