package dr.modification

import dr.spi.Action

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
  override fun toString(): String {
    return "Insert$action - {code=${hashCode()}, table=$table${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }
}

class Update(override val table: String, val id: Long, override val action: Action): InsertOrUpdate() {
  override fun toString(): String {
    return "Update$action - {code=${hashCode()}, table=$table, id=$id${dataText()}${resolvedRefsText()}${unresolvedRefsText()}}"
  }
}

class Delete(override val table: String, val id: Long, override val action: Action): Instruction() {
  override fun toString(): String {
    return "Delete$action - {code=${hashCode()}, table=$table, id=$id}"
  }
}