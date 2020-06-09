package dr.schema.tabular

import dr.adaptor.fn
import dr.adaptor.idFn
import dr.schema.*

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
        else -> topTable.addProperty(TField(field))
      }
    }

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in allOwnedReferences.values) {
      if (oRef.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topTable.addProperty(TEmbedded(oRef, oRef.ref))
      } else {
        // A ref_<rel> --> B
        oRef.ref.getOrCreateTable()
        val dRef = when (oRef.name) {
          SUPER -> TSuperRef(oRef.ref)
          PARENT -> {
            val viaRel = oRef.ref.rels.values.find { it.type == RelationType.OWN && it.ref == this }!!
            val viaRef = if (!viaRel.isCollection) TDirectRef(viaRel.ref, viaRel) else TInverseRef(oRef.ref, viaRel)
            TParentRef(oRef.ref, viaRef)
          }
          else -> TDirectRef(oRef.ref, oRef)
        }
        topTable.addRef(dRef)
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
      val auxTable = linkTable(topTable, lCol)
      auxTable.addTraits(lCol)
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
        //it.addProperty(TID)
        it.addRef(TInverseRef(this, sRelation, false))
        it.addRef(TDirectRef(sRelation.ref, sRelation, false))
        topTable.addManyToMany(sRelation.name, it, sRelation.ref)
      }
    }
  }

  private fun STable.addTraits(rel: SRelation) = rel.traits.values.forEach {
    addProperty(TEmbedded(rel, it))
  }
}