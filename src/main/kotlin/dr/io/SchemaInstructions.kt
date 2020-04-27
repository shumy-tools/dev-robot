package dr.io

class SchemaInstruction(val table: Table) {
  val props: List<TProperty> = mutableListOf<TProperty>()
  val refs: List<TRef> = mutableListOf<TRef>()

  internal fun addProperty(prop: TProperty) {
    (props as MutableList<TProperty>).add(prop)
  }

  internal fun addRef(ref: TRef) {
    (refs as MutableList<TRef>).add(ref)
  }
}
