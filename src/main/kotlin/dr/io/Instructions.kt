package dr.io

import dr.schema.ActionType
import dr.schema.tabular.TProperty
import dr.schema.tabular.TRef
import dr.schema.tabular.Table
import java.util.*

class Instructions(private val root: Instruction, val all: List<Instruction>) {
  val size: Int
    get() = all.size

  val output: Map<String, Any?>
    get() = root.output

  fun exec(eFun: (Instruction) -> Long) {
    // fireCheckedListeners
    /*for (inst in all)
      inst.action.entity.schema.fireListeners(EventType.CHECKED, inst)
    */

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
      inst.unresolvedRefs.forEach { (tRef, refInst) ->
        val refId = ids[refInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $tRef)")
        inst.putResolvedRef(tRef, refId)
      }

      // remove all resolved references
      (inst.unresolvedRefs as LinkedHashMap<TRef, Instruction>).clear()

      val id = eFun(inst)
      ids[inst] = id
      if (inst is Insert)
        inst.setId(id)
    }

    // fireCommittedListeners
    /*for (inst in all)
      inst.action.entity.schema.fireListeners(EventType.COMMITTED, inst)
    */
  }
}


sealed class Instruction {
  abstract val table: Table
  abstract val action: ActionType

  val data: Map<TProperty, Any?> = linkedMapOf()
  val resolvedRefs: Map<TRef, Long?> = linkedMapOf()

  val output: Map<String, Any?> = linkedMapOf()

  fun isEmpty(): Boolean = data.isEmpty() && resolvedRefs.isEmpty() && unresolvedRefs.isEmpty()

  internal val unresolvedRefs: Map<TRef, Instruction> = linkedMapOf()
  private var dataStack = Stack<LinkedHashMap<TProperty, Any?>>()

  init {
    dataStack.push(data as LinkedHashMap<TProperty, Any?>)
  }

  internal fun tableText() = if (table.sRelation == null) table.sEntity.name else "${table.sEntity.name}-${table.sRelation?.name}"
  internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
  internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""
  internal fun unresolvedRefsText() = if (unresolvedRefs.isNotEmpty()) ", urefs=${unresolvedRefs.map { (name, inst) -> "$name=${inst.table}:${inst.hashCode()}" }}" else ""

  internal fun <T: Any> with(level: TProperty, call: () -> T): T {
    val map = linkedMapOf<TProperty, Any?>()

    dataStack.peek()[level] = map
    dataStack.push(map)
      val res = call()
    dataStack.pop()

    return res
  }

  internal fun setId(id: Long?) {
    putOutput("@id", id)
  }

  internal fun putData(key: TProperty, value: Any?) {
    dataStack.peek()[key] = value
  }

  internal fun putOutput(key: String, value: Any?) {
    (output as LinkedHashMap<String, Any?>)[key] = value
  }

  internal fun putAllOutput(values: Map<String, Any?>) {
    (output as LinkedHashMap<String, Any?>).putAll(values)
  }


  internal fun putRef(key: TRef, ref: Instruction) {
    if (ref is Update) {
      (resolvedRefs as LinkedHashMap<TRef, Long?>)[key] = ref.id
    } else {
      (unresolvedRefs as LinkedHashMap<TRef, Instruction>)[key] = ref
    }
  }

  internal fun putResolvedRef(key: TRef, ref: Long?) {
    (resolvedRefs as LinkedHashMap<TRef, Long?>)[key] = ref
  }
}

  class Insert(override val table: Table, override val action: ActionType): Instruction() {
    init { setId(null) } // reserve the first position
    override fun toString() = "Insert($action) - {table=${tableText()}${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Update(override val table: Table, val id: Long, override val action: ActionType): Instruction() {
    override fun toString() = "Update($action) - {table=${tableText()}, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Delete(override val table: Table, override val action: ActionType): Instruction() {
    override fun toString() = "Delete($action) - {table=${tableText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }