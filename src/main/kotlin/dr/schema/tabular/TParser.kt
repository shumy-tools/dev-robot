package dr.schema.tabular

import dr.schema.EntityType
import dr.schema.SEntity
import dr.schema.SRelation
import dr.schema.Schema

class TParser(private val schema: Schema) {
  private val tables = linkedMapOf<String, Table>()

  fun transform(): Tables {
    for (ent in schema.masters.values) {
      ent.getOrCreateTable()
    }

    return Tables(schema, tables)
  }

  private fun SEntity.getOrCreateTable(): Table {
    var isNew = false
    val topInst = tables.getOrPut(this.name) { isNew = true; Table(this) }
    if (isNew)
      processTable(topInst)

    return topInst
  }

  private fun SEntity.processTable(rootInst: Table) {
    processUnpackedTable(rootInst)

    //var topEntity = this
    for (item in sealed.values) {
      item.getOrCreateTable()
      //topEntity = item
    }
  }

  private fun SEntity.processUnpackedTable(topInst: Table) {
    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    for (field in fields.values) {
      when (field.name) {
        ID -> topInst.addProperty(TID)
        TYPE -> topInst.addProperty(TType)
        else -> topInst.addProperty(TField(field))
      }
    }

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in allOwnedReferences.values) {
      if (oRef.isUnique || oRef.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topInst.addProperty(TEmbedded(oRef))
      } else {
        // A ref_<rel> --> B
        oRef.ref.getOrCreateTable()
        topInst.addRef(TDirectRef(oRef.ref, oRef))
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in allOwnedCollections.values) {
      // A <-- inv_<A>_<rel> B
      val refTable = oCol.ref.getOrCreateTable()
      refTable.addRef(TInverseRef(this, oCol))
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in allLinkedReferences.values) {
      // A ref_<rel> --> B
      val refTable = lRef.ref.getOrCreateTable()
      topInst.addRef(TDirectRef(lRef.ref, lRef))
      if (lRef.traits.isNotEmpty()) {
        topInst.addProperty(TEmbedded(lRef))
      }
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in allLinkedCollections.values) {
      val linkInst = linkTable(lCol)
      if (lCol.traits.isNotEmpty()) {
        linkInst.addProperty(TEmbedded(lCol))
      }
    }
  }

  private fun SEntity.linkTable(sRelation: SRelation): Table {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      val refTable = sRelation.ref.getOrCreateTable()
      refTable.addRef(TInverseRef(this, sRelation))
      refTable
    } else {
      // A <-- [inv ref] --> B
      sRelation.ref.getOrCreateTable()
      Table(this, sRelation).also {
        tables[it.name] = it
        it.addProperty(TID)
        it.addRef(TDirectRef(sRelation.ref, sRelation, false))
        it.addRef(TInverseRef(this, sRelation, false))
      }
    }
  }
}