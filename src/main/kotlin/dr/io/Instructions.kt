package dr.io

import dr.schema.ActionType
import dr.schema.EventType
import dr.schema.SEntity
import dr.schema.SRelation
import java.util.*

class Instructions(private val root: Instruction, val all: List<Instruction>) {
  val size: Int
    get() = all.size

  fun exec(eFun: (Instruction) -> Long): Long {
    // fireCheckedListeners
    for (inst in all)
      inst.action.sEntity.fireListeners(EventType.CHECKED, inst)

    val ids = mutableMapOf<Instruction, Long>()

    val head = all.first()
    if (head.unresolvedRefs.isNotEmpty())
      throw Exception("First instruction must have all references already resolved! - (Code bug, please report the issue)")

    ids[head] = eFun(head)

    // execute and resolve children references
    for (inst in all.drop(1)) {
      inst.unresolvedRefs.forEach { (refName, refInst) ->
        val refId = ids[refInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $refName)")
        inst.putResolvedRef(refName, refId)
      }

      // remove all resolved references
      (inst.unresolvedRefs as LinkedHashMap<String, Instruction>).clear()

      ids[inst] = eFun(inst)
    }

    // fireCommittedListeners
    for (inst in all)
      inst.action.sEntity.fireListeners(EventType.COMMITTED, inst)

    return ids[root]!!
  }
}

sealed class Action(val type: ActionType) {
  abstract val sEntity: SEntity
}

  class CreateAction(override val sEntity: SEntity): Action(ActionType.CREATE) {
    override fun toString() = "(CREATE, ${sEntity.name})"
  }

  class UpdateAction(override val sEntity: SEntity): Action(ActionType.UPDATE) {
    override fun toString() = "(UPDATE - ${sEntity.name})"
  }

  class DeleteAction(override val sEntity: SEntity, val id: Long): Action(ActionType.DELETE) {
    override fun toString() = "(DELETE - ${sEntity.name})"
  }

  class AddAction(override val sEntity: SEntity, val sRelation: SRelation): Action(ActionType.ADD) {
    override fun toString() = "(ADD - ${sEntity.name}.${sRelation.name})"
  }

  class LinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(ActionType.LINK) {
    override fun toString() = "(LINK - ${sEntity.name}.${sRelation.name})"
  }

  class UnlinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(ActionType.UNLINK) {
    override fun toString() = "(UNLINK - ${sEntity.name}.${sRelation.name})"
  }


sealed class Instruction {
  abstract val table: String
  abstract val action: Action

  val data: Map<String, Any?> = linkedMapOf()
  val resolvedRefs: Map<String, Long?> = linkedMapOf()
  val unresolvedRefs: Map<String, Instruction> = linkedMapOf()

  fun isEmpty(): Boolean = data.isEmpty() && resolvedRefs.isEmpty() && unresolvedRefs.isEmpty()

  private var dataStack = Stack<LinkedHashMap<String, Any?>>()

  init {
    dataStack.push(data as LinkedHashMap<String, Any?>)
  }

  internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
  internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""
  internal fun unresolvedRefsText() = if (unresolvedRefs.isNotEmpty()) ", urefs=${unresolvedRefs.map { (name, inst) -> "$name=${inst.table}:${inst.hashCode()}" }}" else ""

  internal fun with(level: String, call: () -> Unit) {
    val map = linkedMapOf<String, Any?>()
    with(dataStack) {
      peek()[level] = map
      push(map)
        call()
      pop()
    }
  }

  internal fun putData(name: String, value: Any?) {
    dataStack.peek()[name] = value
  }

  internal fun putResolvedRef(name: String, ref: Long?) {
    (this.resolvedRefs as LinkedHashMap<String, Long?>)[name] = ref
  }

  internal fun putUnresolvedRef(name: String, ref: Instruction) {
    (this.unresolvedRefs as LinkedHashMap<String, Instruction>)[name] = ref
  }
}

  class Insert(override val table: String, override val action: Action): Instruction() {
    override fun toString() = "Insert$action - {table=$table${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Update(override val table: String, val id: Long, override val action: Action): Instruction() {
    override fun toString() = "Update$action - {table=$table, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Delete(override val table: String, override val action: Action): Instruction() {
    override fun toString() = "Delete$action - {table=$table${resolvedRefsText()}${unresolvedRefsText()}}"
  }