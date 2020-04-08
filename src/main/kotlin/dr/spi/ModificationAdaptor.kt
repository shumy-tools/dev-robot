package dr.spi

import dr.schema.ActionType
import dr.schema.ActionType.*
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
      list.forEach { /*TODO: fire listeners */ }
    }

    fun exec(eFun: (Instruction) -> Long): List<Long> {
      // top entity must have all direct references already resolved
      val first = list.first()
      ids[first] = eFun(first)
      println("  (${ids[first]}) -> $first")

      // execute and resolve children references
      for (inst in list.drop(1)) {
        if (inst is InsertOrUpdate) {
          inst.unresolvedRefs.forEach { (refField, toInst) ->
            val refId = ids[toInst] ?: throw Exception("ID not found for reference! - (${inst.table}, $refField)")
            inst.putResolvedRef(refField, refId)
          }
        }

        ids[inst] = eFun(inst)
        println("  (${ids[inst]}) -> $inst")
      }

      return roots.map { ids[it]!! }
    }

    internal fun fireCommittedListeners() {
      list.forEach { /*TODO: fire listeners */ }
    }
  }

    sealed class Action(val type: ActionType)

      data class CreateAction(val sEntity: SEntity): Action(CREATE) {
        override fun toString(): String {
          return "(CREATE, ${sEntity.name})"
        }
      }

      data class UpdateAction(val sEntity: SEntity, val id: Long): Action(UPDATE) {
        override fun toString(): String {
          return "(UPDATE - ${sEntity.name}, $id)"
        }
      }

      data class DeleteAction(val sEntity: SEntity, val id: Long): Action(DELETE) {
        override fun toString(): String {
          return "(UPDATE - ${sEntity.name}, $id)"
        }
      }

      data class AddAction(val sEntity: SEntity, val sRelation: SRelation): Action(ADD) {
        override fun toString(): String {
          return "(ADD - ${sEntity.name}.${sRelation.name})"
        }
      }

      data class LinkAction(val sEntity: SEntity, val sRelation: SRelation): Action(LINK) {
        override fun toString(): String {
          return "(LINK - ${sEntity.name}.${sRelation.name})"
        }
      }

      data class RemoveAction(val sEntity: SEntity, val sRelation: SRelation): Action(REMOVE) {
        override fun toString(): String {
          return "(REMOVE - ${sEntity.name}.${sRelation.name})"
        }
      }


    sealed class Instruction {
      abstract val table: String
      abstract val action: Action
    }

      sealed class InsertOrUpdate(): Instruction() {
        val data: Map<String, Any?> = linkedMapOf()
        val resolvedRefs: Map<String, Long?> = linkedMapOf()
        internal val unresolvedRefs: Map<String, Instruction> = linkedMapOf()

        internal fun dataText() = if (data.isNotEmpty()) ", data=$data" else ""
        internal fun refsText() = if (resolvedRefs.isNotEmpty()) ", refs=$resolvedRefs" else ""

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

      data class Insert(override val table: String, override val action: Action): InsertOrUpdate() {
        override fun toString(): String {
          return "Insert$action - {table=$table${dataText()}${refsText()}}"
        }
      }

      data class Update(override val table: String, val id: Long, override val action: Action): InsertOrUpdate() {
        override fun toString(): String {
          return "Update$action - {table=$table, id=$id${dataText()}${refsText()}}"
        }
      }

      data class Delete(override val table: String, val id: Long, override val action: Action): Instruction() {
        override fun toString(): String {
          return "Delete$action - {table=$table, id=$id}"
        }
      }