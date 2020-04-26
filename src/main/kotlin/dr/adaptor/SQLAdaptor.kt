package dr.adaptor

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dr.io.*
import dr.schema.*
import dr.schema.FieldType.*
import dr.spi.IModificationAdaptor

class SQLAdaptor(val schema: Schema, val url: String): IModificationAdaptor {
  private val config = HikariConfig().also {
    it.jdbcUrl = url
    //it.addDataSourceProperty("cachePrepStmts", "true")
    //it.addDataSourceProperty("prepStmtCacheSize", "250")
    //it.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  }

  private val ds = HikariDataSource(config)

  fun createSchema() {
    val sqlSchema = SchemaCreate(schema).createTables()
    val conn = ds.connection
      sqlSchema.forEach{ tlb ->
        val sql = tlb.create()
        println(sql)
        conn.createStatement().use { it.execute(sql) }
      }

      sqlSchema.forEach{ tlb ->
        tlb.foreignKeys().forEach { sql ->
          println(sql)
          conn.createStatement().use { it.execute(sql) }
        }
      }
    conn.close()
  }

  override fun commit(instructions: Instructions) {
    val conn = ds.connection

    conn.createStatement().use { stmt ->
      //stmt.executeUpdate("DROP TABLE IF EXISTS billionaires;")
    }

    conn.close()
  }
}



/* ------------------------- helpers -------------------------*/
fun table(sEntity: SEntity): String {
  return sEntity.name.replace('.', '_').toLowerCase()
}

class SchemaInstruction(val name: String) {
  private val fields = mutableListOf<String>()
  private val keys = mutableListOf<String>()

  fun addType() {
    fields.add(""""@type" VARCHAR(255) NOT NULL""")
  }

  fun addSuper(sEntity: SEntity) {
    addForeignKey("@super", table(sEntity))
  }

  fun addField(field: SField) {
    val optional = if (!field.isOptional) "NOT NULL" else ""
    fields.add(when (field.type) {
      TEXT -> """"${field.name}" VARCHAR(255) $optional"""
      INT -> """"${field.name}" BIGINT $optional"""
      FLOAT -> """"${field.name}" DOUBLE $optional"""
      BOOL -> """"${field.name}" BOOLEAN $optional"""
      TIME -> """"${field.name}" TIME $optional"""
      DATE -> """"${field.name}" DATE $optional"""
      DATETIME -> """"${field.name}" TIMESTAMP $optional"""
    })
  }

  fun addEmbedded(ref: SRelation) {
    val optional = if (!ref.isOptional) "NOT NULL" else ""
    fields.add(""""${ref.name}" JSON $optional""")
  }

  fun addTraits(ref: SRelation) {
    val optional = if (!ref.isOptional || ref.isCollection) "NOT NULL" else ""
    fields.add(""""@traits__${ref.name}" JSON $optional""")
  }

  fun addForeignKey(refKey: String, refTable: String) {
    fields.add(""""$refKey" BIGINT""")
    keys.add("""
      |ALTER TABLE $name
      |  ADD FOREIGN KEY ("$refKey")
      |  REFERENCES $refTable("@id");
    """.trimMargin())
  }

  fun create(): String {
    val hasFields = if (fields.isNotEmpty()) "," else ""
    return """
      |CREATE TABLE ${name} (
      |  "@id" IDENTITY NOT NULL PRIMARY KEY$hasFields
      |  ${fields.joinToString(separator = ",\n  ")}
      |);
    """.trimMargin()
  }

  fun foreignKeys(): List<String> = keys
}



class SchemaCreate(val schema: Schema) {
  private val tables = linkedMapOf<String, SchemaInstruction>()

  fun createTables(): List<SchemaInstruction> {
    for (ent in schema.masters.values) {
      ent.getOrCreateTable()
    }

    return tables.values.toList()
  }

  private fun SEntity.getOrCreateTable(): SchemaInstruction {
    var isNew = false
    val name = table(this)

    val topInst = tables.getOrPut(name) { isNew = true; SchemaInstruction(name) }
    if (isNew)
      processTable(topInst)

    return topInst
  }

  private fun SEntity.processTable(rootInst: SchemaInstruction) {
    processUnpackedTable(rootInst)

    var topEntity = this
    for (item in sealed.values) {
      item.getOrCreateTable().also { it.addSuper(topEntity) }
      topEntity = item
    }
  }

  private fun SEntity.processUnpackedTable(topInst: SchemaInstruction) {
    if (isSealed)
      topInst.addType()

    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    for (field in fields.values) {
      topInst.addField(field)
    }

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in allOwnedReferences.values) {
      if (oRef.isUnique || oRef.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topInst.addEmbedded(oRef)
      } else {
        // A ref_<rel> --> B
        val refTable = oRef.ref.getOrCreateTable()
        topInst.addForeignKey("@ref__${oRef.name}", refTable.name)
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in allOwnedCollections.values) {
      // A <-- inv_<A>_<rel> B
      val refTable = oCol.ref.getOrCreateTable()
      refTable.addForeignKey("@inv_${topInst.name}__${oCol.name}", topInst.name)
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in allLinkedReferences.values) {
      // A ref_<rel> --> B
      val refTable = lRef.ref.getOrCreateTable()
      topInst.addForeignKey("@ref__${lRef.name}", refTable.name)
      if (lRef.traits.isNotEmpty()) {
        topInst.addTraits(lRef)
      }
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in allLinkedCollections.values) {
      val linkInst = linkTable(lCol)
      if (lCol.traits.isNotEmpty()) {
        linkInst.addTraits(lCol)
      }
    }
  }

  private fun SEntity.linkTable(sRelation: SRelation): SchemaInstruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      val refTable = sRelation.ref.getOrCreateTable()
      refTable.addForeignKey("@inv_${table(this)}__${sRelation.name}", table(this))
      refTable
    } else {
      // A <-- [inv ref] --> B
      val refTable = sRelation.ref.getOrCreateTable()
      SchemaInstruction("${table(this)}__${sRelation.name}").also {
        it.addForeignKey("@inv", table(this))
        it.addForeignKey("@ref", refTable.name)
      }
    }
  }
}