package dr.schema.tabular

import dr.schema.EntityType
import dr.schema.SEntity
import dr.schema.SRelation
import dr.schema.Schema

class TParser(private val schema: Schema) {
  private val tables = linkedMapOf<String, STable>()

  fun transform(): Tables {
    for (ent in schema.masters.values) {
      ent.getOrCreateTable()
    }

    return Tables(schema, tables)
  }

  private fun SEntity.getOrCreateTable(): STable {
    var isNew = false
    val topInst = tables.getOrPut(this.name) { isNew = true; STable(this) }
    if (isNew)
      processTable(topInst)

    return topInst
  }

  private fun SEntity.processTable(rootTable: STable) {
    processUnpackedTable(rootTable)
    sealed.values.forEach { it.getOrCreateTable() }
  }

  private fun SEntity.processUnpackedTable(topTable: STable) {
    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    for (field in fields.values) {
      when (field.name) {
        ID -> topTable.addProperty(TID)
        TYPE -> topTable.addProperty(TType)
        else -> topTable.addProperty(TField(field))
      }
    }

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in allOwnedReferences.values) {
      if (oRef.isUnique || oRef.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topTable.addProperty(TTraits(oRef.ref))
      } else {
        // A ref_<rel> --> B
        oRef.ref.getOrCreateTable()
        topTable.addRef(TDirectRef(oRef.ref, oRef))
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in allOwnedCollections.values) {
      // A <-- inv_<A>_<rel> B
      val refTable = oCol.ref.getOrCreateTable()
      val invRef = TInverseRef(this, oCol)
      topTable.addOneToMany(invRef)
      refTable.addRef(invRef)
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in allLinkedReferences.values) {
      // A ref_<rel> --> B
      lRef.ref.getOrCreateTable()
      topTable.addRef(TDirectRef(lRef.ref, lRef))
      topTable.addTraits(lRef)
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in allLinkedCollections.values) {
      val linkInst = linkTable(topTable, lCol)
      linkInst.addTraits(lCol)
    }
  }

  private fun SEntity.linkTable(topTable: STable, sRelation: SRelation): STable {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      val refTable = sRelation.ref.getOrCreateTable()
      val invRef = TInverseRef(this, sRelation)
      topTable.addOneToMany(invRef)
      refTable.addRef(invRef)

      refTable
    } else {
      // A <-- [inv ref] --> B
      sRelation.ref.getOrCreateTable()
      STable(this, sRelation).also {
        tables[it.name] = it
        it.addProperty(TID)
        it.addRef(TInverseRef(this, sRelation, false))
        it.addRef(TDirectRef(sRelation.ref, sRelation, false))
        topTable.addManyToMany(sRelation.name, it, sRelation.ref)
      }
    }
  }

  private fun STable.addTraits(rel: SRelation) = rel.traits.values.forEach {
    addProperty(TTraits(it))
  }
}