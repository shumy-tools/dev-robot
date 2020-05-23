package dr.adaptor

import com.zaxxer.hikari.HikariDataSource
import dr.JsonParser
import dr.io.Delete
import dr.io.Insert
import dr.io.Instructions
import dr.io.Update
import dr.query.QTree
import dr.schema.Schema
import dr.schema.tabular.*
import dr.spi.IAdaptor
import org.jooq.Constraint
import org.jooq.SQLDialect
import org.jooq.UpdateSetMoreStep
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType
import java.sql.SQLException
import java.sql.Statement

class SQLAdaptor(val schema: Schema, private val url: String): IAdaptor {
  override val tables: Tables = TParser(schema).transform()

  private val ds = HikariDataSource().also {
    it.jdbcUrl = url
    it.addDataSourceProperty("cachePrepStmts", true)
    it.addDataSourceProperty("prepStmtCacheSize", 5*tables.size)
    it.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
  }

  private val settings = Settings()
    .withRenderQuotedNames(RenderQuotedNames.ALWAYS)
    .withExecuteLogging(true)

  private val db = using(ds, SQLDialect.H2, settings)

  init {
    if (db.selectOne().fetch().isEmpty())
      throw SQLException("Database connection failed! - ($url)")
  }

  fun createSchema() {
    val allConstraints = linkedMapOf<STable, List<Constraint>>()
    tables.allTables().forEach { tlb ->
      val constraints = mutableListOf<Constraint>()
      allConstraints[tlb] = constraints

      var dbTable = db.createTable(table(tlb.sqlName()))

      // set all properties
      for (prop in tlb.props.values) {
        if (prop.isUnique) constraints.add(unique(prop.name))

        dbTable = when(prop) {
          is TID -> dbTable.column(prop.name, SQLDataType.BIGINT.nullable(false).identity(true))
          is TType -> dbTable.column(prop.name, SQLDataType.VARCHAR.nullable(false))
          is TEmbedded -> dbTable.column(prop.name, SQLDataType.VARCHAR.nullable(prop.rel.isOptional))
          is TField -> dbTable.column(prop.name, prop.field.type.toSqlType().nullable(prop.field.isOptional))
        }
      }

      // set all references
      for (ref in tlb.refs) {
        val rName = ref.fn(tlb)
        dbTable = dbTable.column(rName, SQLDataType.BIGINT.nullable(ref.isOptional))

        // add ref unique for non auxTable
        if (tlb.sRelation == null && ref.isUnique)
          constraints.add(unique(rName))

        val fk = foreignKey(rName).references(table(ref.refTable.sqlName()))
        constraints.add(fk)
      }

      // set primary key
      val pk = if (tlb.sRelation == null) primaryKey(ID) else primaryKey(INV, REF)
      val dbFinal = dbTable.constraint(pk)

      println(dbFinal.sql)
      dbFinal.execute()
    }

    allConstraints.forEach { (tlb, cList) ->
      val dbAlterTable = db.alterTable(table(tlb.sqlName())).add(cList)
      //println(dbAlterTable.sql)
      dbAlterTable.execute()
    }
  }

  override fun compile(query: QTree) = SQLQueryExecutor(db, tables, query)

  override fun commit(instructions: Instructions) {
    db.transaction { conf ->
      println("TX-START")
      val tx = using(conf)

      instructions.exec { inst ->
        val dataFields = inst.data.values
        val dataRefs = inst.resolvedRefs.values
        val tableName = inst.table.sqlName()

        when (inst) {
          is Insert -> {
            val fields = inst.data.keys.map { it.fn() }
            val refs = inst.resolvedRefs.keys.map { it.fn(inst.table) }

            val insert = tx.insertInto(table(tableName), fields.plus(refs))
              .values(dataFields.plus(dataRefs))
              //.returning(TID.fn())

            print("  ${insert.sql}; ${dataFields.plus(dataRefs)}")

            // FIX: jOOQ is not returning the generated key!!!
            //val id = insert.fetchOne() as Long? ?: throw Exception("Insert failed! No return $ID!")

            val stmt = conf.connectionProvider().acquire().prepareStatement(insert.sql, Statement.RETURN_GENERATED_KEYS)

            var seq = 1
            inst.data.forEach {
              // TODO: parse Trait class directly instead of a map?
              val cValue = if (it.key is TEmbedded && it.value != null) JsonParser.write(it.value!!) else it.value
              stmt.setObject(seq, cValue)
              seq++
            }

            dataRefs.forEach {stmt.setObject(seq, it); seq++}

            val affectedRows = stmt.executeUpdate()
            if (affectedRows == 0)
              throw Exception("Instruction failed, no rows affected!")

            val genKeys = stmt.generatedKeys
            val id = if (genKeys.next()) genKeys.getLong(1) else 0L
            println(" - $ID=$id")

            id
          }

          is Update -> {
            var dbUpdate = tx.update(table(tableName)) as UpdateSetMoreStep<*>

            inst.data.forEach {
              // TODO: parse Trait class directly instead of a map?
              val cValue = if (it.key is TEmbedded && it.value != null) JsonParser.write(it.value!!) else it.value
              dbUpdate = dbUpdate.set(it.key.fn(), cValue)
            }

            inst.resolvedRefs.forEach { dbUpdate = dbUpdate.set(it.key.fn(inst.table), it.value) }

            val update = dbUpdate.where(idFn().eq(inst.refID.id))

            println("  ${update.sql}; ${dataFields.plus(dataRefs)} - @id=${inst.refID.id}")
            val affectedRows = update.execute()
            if (affectedRows == 0)
              throw Exception("Instruction failed, no rows affected!")

            0L
          }

          is Delete -> {
            val delConditions = inst.resolvedRefs.map { it.key.fn(inst.table).eq(it.value) }
            delConditions.drop(1).fold(delConditions.first()) { acc, nxt -> acc.and(nxt) }
            val dbDelete = tx.delete(table(tableName)).where(delConditions)

            println("  ${dbDelete.sql}; $dataRefs")
            val affectedRows = dbDelete.execute()
            if (affectedRows == 0)
              throw Exception("Instruction failed, no rows affected!")

            0L
          }
        }
      }
      println("TX-COMMIT")
    }
  }
}