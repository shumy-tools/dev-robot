package dr.io

import dr.schema.ActionType
import dr.schema.RefID
import dr.schema.tabular.*
import java.util.*

class Instructions(val all: MutableList<Instruction> = mutableListOf()) {
  lateinit var root: Instruction
    internal set

  val output: Map<String, Any?>
    get() = root.output

  fun include(instructions: Instructions) {
    all.addAll(instructions.all)
  }

  fun exec(eFun: (Instruction) -> Long) {
    // fireCheckedListeners
    /*for (inst in all)
      inst.action.entity.schema.fireListeners(EventType.CHECKED, inst)
    */

    val ids = mutableMapOf<Instruction, RefID>()

    val head = all.first()
    if (head.unresolvedRefs.isNotEmpty())
      throw Exception("First instruction must have all references already resolved! - (Code bug, please report the issue)")

    val firstId = eFun(head)
    ids[head] = RefID(firstId)
    if (head is Insert)
      head.setId(firstId)

    // execute and resolve children references
    for (inst in all.drop(1)) {
      inst.unresolvedRefs.forEach { (tRef, refInst) ->
        val refID = ids[refInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $tRef)")
        inst.putResolvedRef(tRef, refID)
      }

      // remove all resolved references
      (inst.unresolvedRefs as LinkedHashMap<TRef, Instruction>).clear()

      val id = eFun(inst)
      ids[inst] = RefID(id)
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
  abstract val table: STable
  abstract val action: ActionType

  private val _resolvedRefs = linkedMapOf<TRef, RefID>()
  private val _unresolvedRefs = linkedMapOf<TRef, Instruction>()
  private var dataStack = Stack<LinkedHashMap<TProperty, Any?>>()

  val refID = RefID()
  val data = linkedMapOf<TProperty, Any?>()
  val output = linkedMapOf<String, Any?>()

  init { dataStack.push(data) }

  val unresolvedRefs: Map<TRef, Instruction>
    get() = _unresolvedRefs

  val resolvedRefs: Map<TRef, Long?>
    get() = _resolvedRefs.map { it.key to it.value.id }.toMap()

  fun isEmpty(): Boolean = data.isEmpty() && _resolvedRefs.isEmpty() && _unresolvedRefs.isEmpty()

  internal fun tableText() = if (table.sRelation == null) table.sEntity.name else "${table.sEntity.name}-${table.sRelation?.name}"
  internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
  internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""
  internal fun unresolvedRefsText() = if (_unresolvedRefs.isNotEmpty()) ", urefs=${_unresolvedRefs.map { (name, inst) -> "$name=${inst.table}:${inst.hashCode()}" }}" else ""

  internal fun <T: Any> with(level: TProperty, call: () -> T): T {
    val map = linkedMapOf<TProperty, Any?>()

    dataStack.peek()[level] = map
    dataStack.push(map)
      val res = call()
    dataStack.pop()

    return res
  }

  internal fun setId(id: Long?) {
    refID.id = id
    putOutput(ID, id)
  }

  internal fun putData(key: TProperty, value: Any?) {
    if(key.name == ID) return // ignore @id as a data value
    dataStack.peek()[key] = value
  }

  internal fun putOutput(key: String, value: Any?) {
    output[key] = value
  }

  internal fun putAllOutput(values: Map<String, Any?>) {
    output.putAll(values)
  }


  internal fun putRef(key: TRef, ref: Instruction) {
    if (ref is Update) {
      _resolvedRefs[key] = ref.refID
    } else {
      _unresolvedRefs[key] = ref
    }
  }

  internal fun putResolvedRef(key: TRef, ref: RefID) {
    _resolvedRefs[key] = ref
  }
}

  class Insert(override val table: STable, override val action: ActionType): Instruction() {
    init { setId(null) } // reserve the first position for @id in the output
    override fun toString() = "Insert($action) - {table=${tableText()}${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Update(override val table: STable, val id: Long?, override val action: ActionType): Instruction() {
    init { setId(id) }
    override fun toString() = "Update($action) - {table=${tableText()}, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }

  class Delete(override val table: STable, override val action: ActionType): Instruction() {
    override fun toString() = "Delete($action) - {table=${tableText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }