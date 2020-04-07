package dr.spi

interface IModificationAdaptor {
  fun commit(instructions: Instructions): Long
}

  class Instructions() {
    internal val list = mutableListOf<Instruction>()

    fun exec(eFun: (Instruction) -> Long): Long {
      val ids = mutableMapOf<Instruction, Long>()

      val first = list.first()
      val id = eFun(first)
      ids[first] = id

      for (inst in list.drop(1)) {
        if (inst is InsertOrUpdate) {
          val mutData = inst.data as MutableMap<String, Any?>
          inst.nativeRefs.forEach { (refField, toInst) ->
            val refId = ids[toInst] ?: throw Exception("ID not found for reference! - (${inst.entity}, $refField)")
            mutData[refField] = refId
          }
        }

        ids[inst] = eFun(inst)
      }

      return id
    }
  }

    sealed class Instruction {
      abstract val entity: String
    }

      sealed class InsertOrUpdate: Instruction() {
        abstract val data: Map<String, Any?>
        internal val nativeRefs = mutableMapOf<String, Instruction>()
      }

      data class Insert(override val entity: String, override val data: Map<String, Any?>): InsertOrUpdate()
      data class Update(override val entity: String, val id: Long, override val data: Map<String, Any?>): InsertOrUpdate()
      data class Delete(override val entity: String, val id: Long): Instruction()