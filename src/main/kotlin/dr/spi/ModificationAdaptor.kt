package dr.spi

import dr.modification.*
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
        override fun toString(): String {
          return "(CREATE, ${sEntity.name})"
        }
      }

      class UpdateAction(override val sEntity: SEntity, val id: Long): Action(UPDATE) {
        override fun toString(): String {
          return "(UPDATE - ${sEntity.name}, $id)"
        }
      }

      class DeleteAction(override val sEntity: SEntity, val id: Long): Action(DELETE) {
        override fun toString(): String {
          return "(DELETE - ${sEntity.name}, $id)"
        }
      }

      class AddAction(override val sEntity: SEntity, val sRelation: SRelation): Action(ADD) {
        override fun toString(): String {
          return "(ADD - ${sEntity.name}.${sRelation.name})"
        }
      }

      class LinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(LINK) {
        override fun toString(): String {
          return "(LINK - ${sEntity.name}.${sRelation.name})"
        }
      }

      class UnlinkAction(override val sEntity: SEntity, val sRelation: SRelation): Action(UNLINK) {
        override fun toString(): String {
          return "(UNLINK - ${sEntity.name}.${sRelation.name})"
        }
      }