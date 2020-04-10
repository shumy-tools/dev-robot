package dr.spi

import dr.schema.ActionType
import dr.schema.ActionType.*
import dr.schema.EventType
import dr.schema.SEntity
import dr.schema.SRelation

interface IModificationAdaptor {
  fun commit(instructions: Instructions): List<Long>
}

class Instructions() {
  internal val roots = mutableListOf<Instruction>()
  private val list = mutableListOf<Instruction>()
  private val ids = mutableMapOf<Instruction, Long>()

  val size: Int
    get() = list.size

  constructor(first: Instruction): this() {
    roots.add(first)
  }

  internal fun addInstruction(inst: Instruction) {
    list.add(inst)
  }

  internal fun addInstruction(index: Int, inst: Instruction) {
    list.add(index, inst)
  }

  internal fun fireCheckedListeners() {
    for (inst in list)
      inst.action.sEntity.fireListeners(EventType.CHECKED, inst)
  }

  fun exec(eFun: (Instruction) -> Long): List<Long> {
    val first = list.first()
    ids[first] = eFun(first)

    if (first is InsertOrUpdate && first.unresolvedRefs.isNotEmpty())
      throw Exception("First instruction must have all references already resolved! - (Code bug, please report the issue)")

    // execute and resolve children references
    for (inst in list.drop(1)) {
      if (inst is InsertOrUpdate) {
        inst.unresolvedRefs.forEach { (refName, refInst) ->
          val refId = ids[refInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $refName)")
          inst.putResolvedRef(refName, refId)
        }

        // remove all resolved references
        (inst.unresolvedRefs as LinkedHashMap<String, Instruction>).clear()
      }

      ids[inst] = eFun(inst)
    }

    return roots.map { ids[it]!! }
  }

  internal fun fireCommittedListeners() {
    for (inst in list)
      inst.action.sEntity.fireListeners(EventType.COMMITTED, inst, ids[inst])
  }
}

sealed class Action(val type: ActionType) {
  abstract val sEntity: SEntity
}

  class CreateAction(override val sEntity: SEntity): Action(CREATE) {
    override fun toString() = "(CREATE, ${sEntity.name})"
  }

  class UpdateAction(override val sEntity: SEntity, val id: Long): Action(UPDATE) {
    override fun toString() = "(UPDATE - ${sEntity.name}, $id)"
  }

  class DeleteAction(override val sEntity: SEntity, val id: Long): Action(DELETE) {
    override fun toString() = "(DELETE - ${sEntity.name}, $id)"
  }

  class AddAction(override val sEntity: SEntity, val sRelation: SRelation): Action(ADD) {
    override fun toString() = "(ADD - ${sEntity.name}.${sRelation.name})"
  }

  class LinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(LINK) {
    override fun toString() = "(LINK - ${sEntity.name}.${sRelation.name})"
  }

  class UnlinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(UNLINK) {
    override fun toString() = "(UNLINK - ${sEntity.name}.${sRelation.name})"
  }

sealed class Instruction {
  abstract val table: String
  abstract val action: Action
}

  sealed class InsertOrUpdate(): Instruction() {
    val data: Map<String, Any?> = linkedMapOf()
    val resolvedRefs: Map<String, Long?> = linkedMapOf()
    val unresolvedRefs: Map<String, Instruction> = linkedMapOf()

    internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
    internal fun resolvedRefsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""
    internal fun unresolvedRefsText() = if (unresolvedRefs.isNotEmpty()) ", urefs=${unresolvedRefs.map { (name, inst) -> "$name=${inst.table}:${inst.hashCode()}" }}" else ""

    internal fun putData(name: String, value: Any?) {
      (this.data as LinkedHashMap<String, Any?>)[name] = value
    }

    internal fun putResolvedRef(name: String, ref: Long?) {
      (this.resolvedRefs as LinkedHashMap<String, Long?>)[name] = ref
    }

    internal fun putUnresolvedRef(name: String, ref: Instruction) {
      (this.unresolvedRefs as LinkedHashMap<String, Instruction>)[name] = ref
    }
  }

    class Insert(override val table: String, override val action: Action): InsertOrUpdate() {
      override fun toString() = "Insert$action - {code=${hashCode()}, table=$table${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
    }

    class Update(override val table: String, val id: Long, override val action: Action): InsertOrUpdate() {
      override fun toString() = "Update$action - {code=${hashCode()}, table=$table, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
    }

    class Delete(override val table: String, val id: Long, override val action: Action): Instruction() {
      override fun toString() = "Delete$action - {code=${hashCode()}, table=$table, id=$id}"
    }