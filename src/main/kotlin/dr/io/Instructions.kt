package dr.io

import dr.schema.ActionType
import dr.schema.RefID
import dr.schema.tabular.ID
import dr.schema.tabular.STable
import dr.schema.tabular.TProperty
import dr.schema.tabular.TRef
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
    val ids = mutableMapOf<Instruction, RefID>()
    for (inst in all) {
      ids[inst] = inst.refID

      // ignore empty updates
      if (inst is Update && inst.data.isEmpty() && inst.resolvedRefs.isEmpty())
        continue

      val id = eFun(inst)
      if (inst is Insert)
        inst.refID.id = id
    }
  }
}


sealed class Instruction(val refID: RefID) {
  abstract val table: STable
  abstract val action: ActionType

  private val _resolvedRefs = linkedMapOf<TRef, RefID>()
  private var dataStack = Stack<LinkedHashMap<TProperty, Any?>>()

  val data = linkedMapOf<TProperty, Any?>()
  val output = linkedMapOf<String, Any?>()

  init { dataStack.push(data) }

  val resolvedRefs: Map<TRef, Long?>
    get() = _resolvedRefs.map { it.key to it.value.id }.toMap()

  internal fun tableText() = table.name
  internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
  internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""

  internal fun <T: Any> with(level: TProperty, call: () -> T): T {
    val map = linkedMapOf<TProperty, Any?>()

    dataStack.peek()[level] = map
    dataStack.push(map)
      val res = call()
    dataStack.pop()

    return res
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

  internal fun putRef(key: TRef, ref: RefID) {
    _resolvedRefs[key] = ref
  }
}

  class Insert(refID: RefID, override val table: STable, override val action: ActionType): Instruction(refID) {
    init { if (table.sRelation == null) putOutput(ID, null) } // reserve the first position for @id in the output
    override fun toString() = "Insert($action) - {table=${tableText()}${dataText()}${resolvedRefsText()}}"
  }

  class Update(refID: RefID, override val table: STable, override val action: ActionType): Instruction(refID) {
    init { if (table.sRelation == null) putOutput(ID, refID.id) }
    override fun toString() = "Update($action) - {table=${tableText()}, id=${refID.id}${dataText()}${resolvedRefsText()}}"
  }

  class Delete(refID: RefID, override val table: STable, override val action: ActionType): Instruction(refID) {
    override fun toString() = "Delete($action) - {table=${tableText()}${resolvedRefsText()}}"
  }