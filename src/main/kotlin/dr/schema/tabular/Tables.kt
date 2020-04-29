package dr.schema.tabular

import dr.schema.SEntity
import dr.schema.SField
import dr.schema.SRelation
import dr.schema.Schema

const val ID = "@id"
const val TYPE = "@type"
const val STATE = "@state"
const val SUPER = "@super"

data class Tables(val schema: Schema, private val tables: Map<String, Table>) {
  val size: Int
    get() = tables.size

  fun allTables(): List<Table> = tables.values.toList()

  fun get(ent: String): Table {
    val tName = tableNameFrom(ent)
    return tables[tName] ?: throw Exception("No table found: $tName")
  }

  fun get(ent: SEntity, rel: SRelation? = null): Table {
    val tName = tableNameFrom(ent.name, rel?.name)
    return tables.getValue(tName)
  }
}

data class Table(val sEntity: SEntity, val sRelation: SRelation? = null) {
  val props: List<TProperty> = mutableListOf()
  val refs: List<TRef> = mutableListOf()

  val name: String by lazy {
    tableNameFrom(sEntity.name, sRelation?.name)
  }

  internal fun addProperty(prop: TProperty) {
    (props as MutableList<TProperty>).add(prop)
  }

  internal fun addRef(ref: TRef) {
    (refs as MutableList<TRef>).add(ref)
  }
}

sealed class TProperty {
  override fun toString() = when(this) {
    is TType -> TYPE
    is TEmbedded -> "@${rel.name}"
    is TField -> field.name
  }
}

  object TType: TProperty()
  class TEmbedded(val rel: SRelation): TProperty()
  class TField(val field: SField): TProperty()


sealed class TRef {
  abstract val refEntity: SEntity

  val refTable: Table
    get() = Table(refEntity)

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
