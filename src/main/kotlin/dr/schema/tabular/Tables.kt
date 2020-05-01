package dr.schema.tabular

import dr.schema.SEntity
import dr.schema.SField
import dr.schema.SRelation
import dr.schema.Schema

const val ID = "@id"
const val TYPE = "@type"
const val STATE = "@state"
const val SUPER = "@super"

data class Tables(val schema: Schema, private val tables: Map<String, STable>) {
  val size: Int
    get() = tables.size

  fun allTables(): List<STable> = tables.values.toList()

  fun get(ent: String): STable {
    val tName = tableNameFrom(ent)
    return tables[tName] ?: throw Exception("No table found: $tName")
  }

  fun get(ent: SEntity, rel: SRelation? = null): STable {
    val tName = tableNameFrom(ent.name, rel?.name)
    return tables.getValue(tName)
  }
}

data class STable(val sEntity: SEntity, val sRelation: SRelation? = null) {
  val props: List<TProperty> = mutableListOf()
  val refs: List<TRef> = mutableListOf()

  val inverseRefs: Map<String, TInverseRef> = linkedMapOf()

  val directRefs: Map<String, TDirectRef> by lazy {
    refs.filterIsInstance<TDirectRef>().map { it.rel.name to it }.toMap()
  }

  val name: String by lazy {
    tableNameFrom(sEntity.name, sRelation?.name)
  }

  internal fun addProperty(prop: TProperty) {
    (props as MutableList<TProperty>).add(prop)
  }

  internal fun addRef(ref: TRef) {
    (refs as MutableList<TRef>).add(ref)
  }

  internal fun addInvRef(invRef: TInverseRef) {
    (inverseRefs as LinkedHashMap<String, TInverseRef>)[invRef.rel.name] = invRef
  }
}

sealed class TProperty {
  val jType: Class<out Any> by lazy {
    when(this) {
      is TID -> java.lang.Long::class.java
      is TType -> java.lang.String::class.java
      is TEmbedded -> java.lang.String::class.java
      is TField -> field.jType
    }
  }

  val name: String by lazy {
    when(this) {
      is TID -> ID
      is TType -> TYPE
      is TEmbedded -> "@${rel.name}"
      is TField -> field.name
    }
  }

  val isUnique: Boolean by lazy {
    when (this) {
      is TID -> true
      is TType -> false
      is TEmbedded -> false
      is TField -> field.isUnique
    }
  }

  override fun toString() = name
}

  object TID: TProperty()
  object TType: TProperty()
  class TEmbedded(val rel: SRelation): TProperty()
  class TField(val field: SField): TProperty()


sealed class TRef {
  abstract val refEntity: SEntity

  val refTable: STable
    get() = STable(refEntity)

  val isUnique: Boolean by lazy {
    when (this) {
      is TSuperRef -> true
      is TDirectRef -> rel.isUnique
      is TInverseRef -> rel.isUnique
    }
  }

  override fun toString() = when(this) {
    is TSuperRef -> SUPER
    is TDirectRef -> if (!includeRelName) "@ref-to-${refEntity.name}" else "@ref-to-${refEntity.name}-${rel.name}"
    is TInverseRef -> if (!includeRelName) "@inv-to-${refEntity.name}" else "@inv-to-${refEntity.name}-${rel.name}"
  }
}

  class TSuperRef(override val refEntity: SEntity): TRef()
  class TDirectRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()
  class TInverseRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()

/* ------------------------- helpers -------------------------*/
private fun tableNameFrom(ent: String, rel: String? = null) = if (rel == null) ent else "${ent}-${rel}"
