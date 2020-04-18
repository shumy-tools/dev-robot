package dr.io

import dr.schema.ActionType
import dr.schema.EventType
import dr.schema.SRelation
import java.util.*

class Instructions(private val root: Instruction, val all: List<Instruction>) {
  val size: Int
    get() = all.size

  val output: Map<String, Any?>
    get() = root.output

  fun exec(eFun: (Instruction) -> Long) {
    // fireCheckedListeners
    for (inst in all)
      inst.action.entity.schema.fireListeners(EventType.CHECKED, inst)

    val ids = mutableMapOf<Instruction, Long>()

    val head = all.first()
    if (head.unresolvedRefs.isNotEmpty())
      throw Exception("First instruction must have all references already resolved! - (Code bug, please report the issue)")

    val firstId = eFun(head)
    ids[head] = firstId
    if (head is Insert)
      head.setId(firstId)

    // execute and resolve children references
    for (inst in all.drop(1)) {
      inst.unresolvedRefs.forEach { (refName, refInst) ->
        val refId = ids[refInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $refName)")
        inst.putResolvedRef(refName, refId)
      }

      // remove all resolved references
      (inst.unresolvedRefs as LinkedHashMap<String, Instruction>).clear()

      val id = eFun(inst)
      ids[inst] = id
      if (inst is Insert)
        inst.setId(id)
    }

    // fireCommittedListeners
    for (inst in all)
      inst.action.entity.schema.fireListeners(EventType.COMMITTED, inst)
  }
}

sealed class Action(val type: ActionType) {
  abstract val entity: DEntity
}

  class CreateAction(override val entity: DEntity): Action(ActionType.CREATE) {
    override fun toString() = "(CREATE, ${entity.name})"
  }

  class UpdateAction(override val entity: DEntity, val id: Long): Action(ActionType.UPDATE) {
    override fun toString() = "(UPDATE - ${entity.name})"
  }

  class DeleteAction(override val entity: DEntity, val id: Long): Action(ActionType.DELETE) {
    override fun toString() = "(DELETE - ${entity.name})"
  }

  class AddAction(override val entity: DEntity, val sRelation: SRelation): Action(ActionType.ADD) {
    override fun toString() = "(ADD - ${entity.name}.${sRelation.name})"
  }

  class LinkAction(override val entity: DEntity, val sRelation: SRelation): Action(ActionType.LINK) {
    override fun toString() = "(LINK - ${entity.name}.${sRelation.name})"
  }

  class UnlinkAction(override val entity: DEntity, val sRelation: SRelation): Action(ActionType.UNLINK) {
    override fun toString() = "(UNLINK - ${entity.name}.${sRelation.name})"
  }


sealed class Instruction {
  abstract val table: String
  abstract val action: Action

  val data: Map<String, Any?> = linkedMapOf()
  val resolvedRefs: Map<String, Long?> = linkedMapOf()
  val unresolvedRefs: Map<String, Instruction> = linkedMapOf()
  val output: Map<String, Any?> = linkedMapOf()

  fun isEmpty(): Boolean = data.isEmpty() && resolvedRefs.isEmpty() && unresolvedRefs.isEmpty()

  private var dataStack = Stack<LinkedHashMap<String, Any?>>()

  init {
    dataStack.push(data as LinkedHashMap<String, Any?>)
  }

  internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
  internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""
  internal fun unresolvedRefsText() = if (unresolvedRefs.isNotEmpty()) ", urefs=${unresolvedRefs.map { (name, inst) -> "$name=${inst.table}:${inst.hashCode()}" }}" else ""

  internal fun <T: Any> with(level: String, call: () -> T): T {
    val map = linkedMapOf<String, Any?>()

    dataStack.peek()[level] = map
    dataStack.push(map)
      val res = call()
    dataStack.pop()

    return res
  }

  internal fun putData(key: String, value: Any?) {
    dataStack.peek()[key] = value
  }

  internal fun putRef(key: String, ref: Instruction) {
    if (ref is Update) {
      (this.resolvedRefs as LinkedHashMap<String, Long?>)[key] = ref.id
    } else {
      (this.unresolvedRefs as LinkedHashMap<String, Instruction>)[key] = ref
    }
  }

  internal fun putResolvedRef(key: String, ref: Long?) {
    (this.resolvedRefs as LinkedHashMap<String, Long?>)[key] = ref
  }

  internal fun setId(id: Long?) {
    putOutput("@id", id)
  }

  internal fun putOutput(key: String, value: Any?) {
    (this.output as LinkedHashMap<String, Any?>)[key] = value
  }

  internal fun putAllOutput(values: Map<String, Any?>) {
    (this.output as LinkedHashMap<String, Any?>).putAll(values)
  }
}

  class Insert(override val table: String, override val action: Action): Instruction() {
    init { setId(null) } // reserve the first position
    override fun toString() = "Insert$action - {table=$table${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Update(override val table: String, val id: Long, override val action: Action): Instruction() {
    override fun toString() = "Update$action - {table=$table, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Delete(override val table: String, override val action: Action): Instruction() {
    override fun toString() = "Delete$action - {table=$table${resolvedRefsText()}${unresolvedRefsText()}}"
  }