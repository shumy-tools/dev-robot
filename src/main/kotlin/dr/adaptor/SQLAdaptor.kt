package dr.adaptor

import com.zaxxer.hikari.HikariDataSource
import dr.io.Instructions
import dr.query.QSelect
import dr.query.QTree
import dr.schema.FieldType.*
import dr.schema.Schema
import dr.schema.tabular.ID
import dr.schema.tabular.TParser
import dr.schema.tabular.Table
import dr.schema.tabular.Tables
import dr.spi.IAdaptor
import dr.spi.IQueryExecutor
import dr.spi.IResult
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

class SQLAdaptor(val schema: Schema, private val url: String): IAdaptor {
  private val tables: Tables = TParser(schema).transform()

  private val ds = HikariDataSource().also {
    it.jdbcUrl = url
    it.addDataSourceProperty("cachePrepStmts", true)
    it.addDataSourceProperty("prepStmtCacheSize", 5*tables.size)
    it.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
  }

  init {
    ds.connection.use { conn ->
      val stmt = conn.prepareStatement(testQuery())
      if(!stmt.executeQuery().next())
        throw SQLException("Database connection failed! - ($url)")
    }
  }

  fun createSchema() {
    ds.connection.use { conn ->
      tables.allTables().forEach { inst ->
        val sql = inst.createSql()
        println(sql)
        conn.createStatement().use { it.execute(sql) }
      }

      tables.allTables().forEach { inst ->
        inst.refs.forEach { ref ->
          val sql = inst.foreignKeySql(ref)
          println(sql)
          conn.createStatement().use { it.execute(sql) }
        }
      }
    }
  }

  override fun tables(): Tables = tables

  override fun commit(instructions: Instructions) {
    val conn = ds.connection
    try {
      conn.autoCommit = false
      println("TX-START")

      instructions.exec { inst ->
        val sql = inst.sql()
        val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)

        val values = mutableListOf<Any?>()
        var seq = 1
        for (value in inst.data.values.plus(inst.resolvedRefs.values)) {
          values.add(value)
          stmt.setObject(seq, value)
          seq++
        }

        print("  SQL -> $sql $values")
        val affectedRows = stmt.executeUpdate()
        if (affectedRows == 0)
          throw SQLException("Instruction failed, no rows affected - $inst")

        val genKeys = stmt.generatedKeys
        val id = if (genKeys.next()) genKeys.getLong(1) else 0L
        if (id != 0L) println(" - $ID=$id") else println()

        id
      }

      conn.commit()
      println("TX-COMMIT")
    } catch (ex: Exception) {
      conn.rollback()
    } finally {
      conn.close()
    }
  }

  override fun compile(query: QTree): IQueryExecutor {
    val table = tables.get(query.entity)
    val mapper = mutableListOf<String>()
    val mainSql = query.run { table.select(mapper, select, filter, limit, page) }

    val main = SubQuery(mainSql, table, query.select, mapper)

    return MultiQueryExecutor(ds, main, emptyMap())
  }
}

/* ------------------------- helpers -------------------------*/
typealias Rows = List<Map<String, Any?>>

private class SubQuery(val sql: String, val table: Table, val select: QSelect, val mapper: List<String>) {
  fun exec(conn: Connection, params: Map<String, Any>): Rows {
    println("SUB-QUERY: $sql")
    val stmt = conn.prepareStatement(testQuery())

    var seq = 1
    for(param in mapper) {
      val value = params.getValue(param)
      stmt.setObject(seq, value)
      seq++
    }

    val rs = stmt.executeQuery()
    println("(columns=${rs.metaData.columnCount})")

    val result = mutableListOf<Map<String, Any?>>()
    while (rs.next()) {
      val row = linkedMapOf<String, Any?>()
      result.add(row)

      var rSeq = 1
      row[SQL_ID] = rs.getLong(rSeq)

      select.fields.forEach {
        rSeq++
        val sField = table.sEntity.fields.getValue(it.name)
        row[it.name] = when(sField.type) {
          TEXT -> rs.getString(rSeq)
          INT -> rs.getInt(rSeq)
          LONG -> rs.getLong(rSeq)
          FLOAT -> rs.getFloat(rSeq)
          DOUBLE -> rs.getDouble(rSeq)
          BOOL -> rs.getBoolean(rSeq)
          TIME -> rs.getTime(rSeq).toLocalTime()
          DATE -> rs.getDate(rSeq).toLocalDate()
          DATETIME -> rs.getTimestamp(rSeq).toLocalDateTime()
        }
      }
    }

    return result
  }
}

private class MultiQueryExecutor(val ds: HikariDataSource, val main: SubQuery, val queries: Map<String, SubQuery>): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    ds.connection.use { conn ->
      //for ((rel, sQuery) in queries)
      val mRes = main.exec(conn, params)
      println(mRes)

      return SQLResult(mRes, emptyMap())
    }
  }
}

private class SQLResult(val main: Rows, val rs: Map<String, Rows>): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw(): Map<String, Any?> {
    return emptyMap()
  }
}


