package dr.spi

interface IModificationAdaptor {
  fun commit(instructions: Instructions): List<Long>
}

  class Instructions() {
    internal val roots = mutableListOf<Instruction>()
    internal val list = mutableListOf<Instruction>()

    constructor(first: Instruction): this() {
      roots.add(first)
    }

    fun exec(eFun: (Instruction) -> Long): List<Long> {
      val ids = mutableMapOf<Instruction, Long>()

      // top entity must have all direct references already resolved
      val first = list.first()
      val id = eFun(first)
      ids[first] = id

      // execute and resolve children references
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

      return roots.map { ids[it]!! }
    }
  }

    sealed class Instruction {
      abstract val entity: String
    }

      sealed class InsertOrUpdate(): Instruction() {
        abstract val data: Map<String, Any?>
        internal val nativeRefs = mutableMapOf<String, Instruction>()

        internal fun putData(prop: String, value: Any?) {
          (this.data as LinkedHashMap<String, Any?>)[prop] = value
        }

        internal fun putAllData(data: Map<String, Any?>) {
          (this.data as LinkedHashMap<String, Any?>).putAll(data)
        }
      }

      data class Insert(override val entity: String, override val data: Map<String, Any?> = linkedMapOf()): InsertOrUpdate()
      data class Update(override val entity: String, val id: Long, override val data: Map<String, Any?> = linkedMapOf()): InsertOrUpdate()
      data class Delete(override val entity: String, val id: Long): Instruction()