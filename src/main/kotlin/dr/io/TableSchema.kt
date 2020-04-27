package dr.io

import dr.adaptor.refSqlName
import dr.adaptor.sqlName
import dr.schema.SEntity
import dr.schema.SField
import dr.schema.SRelation

const val ID = "@id"

data class Table(val sEntity: SEntity, val sRelation: SRelation? = null)

sealed class TProperty {
  fun name() = when(this) {
    is TType -> "@type"
    is TEmbedded -> "@${rel.name}"
    is TField -> field.name
  }

  override fun toString() = name()
}

  object TType: TProperty()
  class TEmbedded(val rel: SRelation): TProperty()
  class TField(val field: SField): TProperty()

sealed class TRef {
  abstract val refEntity: SEntity

  val refTable: Table
    get() = Table(refEntity)

  fun isUnique() = when (this) {
    is TSuperRef -> true
    is TDirectRef -> rel.isUnique
    is TInverseRef -> rel.isUnique
  }

  fun name() = when(this) {
    is TSuperRef -> "@super"
    is TDirectRef -> if (!includeRelName) "@ref-to-${refEntity.name}" else "@ref-to-${refEntity.name}-${rel.name}"
    is TInverseRef -> if (!includeRelName) "@inv-to-${refEntity.name}" else "@inv-to-${refEntity.name}-${rel.name}"
  }

  override fun toString() = name()
}

  class TSuperRef(override val refEntity: SEntity): TRef()
  class TDirectRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()
  class TInverseRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()