package dr.schema.tabular

import dr.schema.*

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
  val name: String by lazy {
    tableNameFrom(sEntity.name, sRelation?.name)
  }

  val props: Map<String, TProperty> = linkedMapOf()
  val refs: List<TRef> = mutableListOf()

  var superRef: TSuperRef? = null
    internal set

  var parentRef: TParentRef? = null
    internal set

  val oneToOne: Map<String, TDirectRef> = linkedMapOf()
  val oneToMany: Map<String, TInverseRef> = linkedMapOf()
  val manyToMany: Map<String, Pair<STable, SEntity>> = linkedMapOf()

  internal fun addProperty(prop: TProperty) {
    (props as LinkedHashMap<String, TProperty>)[prop.name] = prop
  }

  internal fun addRef(ref: TRef) {
    if (ref !is TParentRef)
      (refs as MutableList<TRef>).add(ref)

    when (ref) {
      is TSuperRef -> superRef = ref
      is TParentRef -> parentRef = ref
      is TDirectRef -> (oneToOne as LinkedHashMap<String, TDirectRef>)[ref.rel.name] = ref
      else -> Unit
    }
  }

  internal fun addOneToMany(invRef: TInverseRef) {
    (oneToMany as LinkedHashMap<String, TInverseRef>)[invRef.rel.name] = invRef
  }

  internal fun addManyToMany(name: String, auxTable: STable, refTable: SEntity) {
    (manyToMany as LinkedHashMap<String, Pair<STable, SEntity>>)[name] = Pair(auxTable, refTable)
  }

  override fun toString() = name
}

sealed class TProperty {
  val jType: Class<out Any> by lazy {
    when(this) {
      is TID -> java.lang.Long::class.java
      is TEmbedded -> java.lang.String::class.java
      is TField -> field.jType
    }
  }

  val name: String by lazy {
    when(this) {
      is TID -> ID
      is TEmbedded -> if (rel.type == RelationType.LINK && rel.isCollection) "$TRAITS${trait.name}" else "$TRAITS${trait.name}@${rel.name}"
      is TField -> field.name
    }
  }

  val isUnique: Boolean by lazy {
    when (this) {
      is TID -> true
      is TEmbedded -> false
      is TField -> field.isUnique
    }
  }

  override fun toString() = name
}

  object TID: TProperty()
  class TEmbedded(val rel: SRelation, val trait: SEntity): TProperty()
  class TField(val field: SField): TProperty()


sealed class TRef {
  abstract val refEntity: SEntity

  val refTable: STable
    get() = STable(refEntity)

  val isUnique: Boolean by lazy {
    when (this) {
      is TSuperRef -> true
      is TParentRef -> false
      is TDirectRef -> rel.isUnique
      is TInverseRef -> false
    }
  }

  val isOptional: Boolean by lazy {
    when (this) {
      is TSuperRef -> false
      is TParentRef -> false
      is TDirectRef -> rel.isOptional
      is TInverseRef -> rel.isOptional || rel.isUnique || rel.type == RelationType.OWN // unique @inv are implemented via nullable reference
    }
  }

  override fun toString() = when(this) {
    is TSuperRef -> SUPER
    is TParentRef -> PARENT
    is TDirectRef -> if (!includeRelName) "$REF-to-${rel.ref.name}" else "$REF-to-${rel.ref.name}-${rel.name}"
    is TInverseRef -> if (!includeRelName) "$INV-to-${refEntity.name}" else "$INV-to-${refEntity.name}-${rel.name}"
  }
}

  class TSuperRef(override val refEntity: SEntity): TRef()
  class TParentRef(override val refEntity: SEntity, val viaRef: TRef): TRef()
  class TDirectRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()
  class TInverseRef(override val refEntity: SEntity, val rel: SRelation, val includeRelName: Boolean = true): TRef()

/* ------------------------- helpers -------------------------*/
private fun tableNameFrom(ent: String, rel: String? = null): String {
  return if (rel == null) ent else if (rel.startsWith(SPECIAL)) "${ent}-${rel.substring(1)}" else "${ent}-${rel}"
}
