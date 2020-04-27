package dr.io

import dr.schema.EntityType
import dr.schema.SEntity
import dr.schema.SRelation
import dr.schema.Schema

class SchemaInstructionBuilder(private val schema: Schema) {
  private val tables = linkedMapOf<Table, SchemaInstruction>()

  fun transform(): List<SchemaInstruction> {
    for (ent in schema.masters.values) {
      ent.getOrCreateTable()
    }

    return tables.values.toList()
  }

  private fun SEntity.getOrCreateTable(): SchemaInstruction {
    var isNew = false
    val table = Table(this)

    val topInst = tables.getOrPut(table) { isNew = true; SchemaInstruction(table) }
    if (isNew)
      processTable(topInst)

    return topInst
  }

  private fun SEntity.processTable(rootInst: SchemaInstruction) {
    processUnpackedTable(rootInst)

    var topEntity = this
    for (item in sealed.values) {
      item.getOrCreateTable().also { it.addRef(TSuperRef(topEntity)) }
      topEntity = item
    }
  }

  private fun SEntity.processUnpackedTable(topInst: SchemaInstruction) {
    if (isSealed)
      topInst.addProperty(TType)

    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    for (field in fields.values) {
      topInst.addProperty(TField(field))
    }

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in allOwnedReferences.values) {
      if (oRef.isUnique || oRef.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topInst.addProperty(TEmbedded(oRef))
      } else {
        // A ref_<rel> --> B
        oRef.ref.getOrCreateTable()
        topInst.addRef(TDirectRef(this, oRef))
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
      topInst.addRef(TDirectRef(this, lRef))
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

  private fun SEntity.linkTable(sRelation: SRelation): SchemaInstruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      val refTable = sRelation.ref.getOrCreateTable()
      refTable.addRef(TInverseRef(this, sRelation))
      refTable
    } else {
      // A <-- [inv ref] --> B
      sRelation.ref.getOrCreateTable()
      SchemaInstruction(Table(this, sRelation)).also {
        it.addRef(TDirectRef(sRelation.ref))
        it.addRef(TInverseRef(this))
      }
    }
  }
}