package dr.adaptor

import com.zaxxer.hikari.HikariDataSource
import dr.io.Instructions
import dr.query.ParamType
import dr.query.QSelect
import dr.query.QTree
import dr.schema.FieldType
import dr.schema.FieldType.*
import dr.schema.Schema
import dr.schema.tabular.ID
import dr.schema.tabular.TParser
import dr.schema.tabular.Table
import dr.schema.tabular.Tables
import dr.spi.IAdaptor
import dr.spi.IQueryExecutor
import dr.spi.IResult
import dr.spi.Rows
import java.sql.Connection
import java.sql.ResultSet
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

        print("  SQL: $sql $values")
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
      println()
      conn.rollback()
      throw ex
    } finally {
      conn.close()
    }
  }

  override fun compile(query: QTree): IQueryExecutor {
    val table = tables.get(query.entity)
    val mapper = query.run { table.select(select, filter, limit, page) }
    val main = SubQuery(mapper, query.select)

    return MultiQueryExecutor(ds, main, emptyMap())
  }
}

/* ------------------------- helpers -------------------------*/
fun ResultSet.getField(seq: Int, type: FieldType): Any? = when(type) {
  TEXT -> getString(seq)
  INT -> getInt(seq)
  LONG -> getLong(seq)
  FLOAT -> getFloat(seq)
  DOUBLE -> getDouble(seq)
  BOOL -> getBoolean(seq)
  TIME -> getTime(seq).toLocalTime()
  DATE -> getDate(seq).toLocalDate()
  DATETIME -> getTimestamp(seq).toLocalDateTime()
}

private class SubQuery(val mapper: QueryMapper, val select: QSelect) {
  fun exec(conn: Connection, params: Map<String, Any>): Rows {
    print("SUB-QUERY: ${mapper.sql} [")
    val stmt = conn.prepareStatement(mapper.sql)

    var seq = 1
    for((name, param) in mapper.params) {
      val value = if (param.type == ParamType.PARAM) params.getValue(name) else param.value
      print(value)
      stmt.setObject(seq, value)

      if (seq < mapper.params.size) print(", ")
      seq++
    }
    println("]")

    val rs = stmt.executeQuery()
    val result = mutableListOf<Map<String, Any?>>()
    while (rs.next()) {
      val row = linkedMapOf<String, Any?>()
      result.add(row)

      var rSeq = 1
      row[SQL_ID] = rs.getLong(rSeq)

      val fields = if (select.hasAll) {
        mapper.schema.fields.map { it.key }
      } else {
        select.fields.map { it.name }
      }

      fields.forEach {
        rSeq++
        val sField = mapper.schema.fields.getValue(it)
        row[it] = rs.getField(rSeq, sField.type)
      }
    }

    println("  $result")
    return result
  }
}

private class MultiQueryExecutor(val ds: HikariDataSource, val main: SubQuery, val queries: Map<String, SubQuery>): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    ds.connection.use { conn ->
      val mRes = main.exec(conn, params)

      //for ((rel, sQuery) in queries)

      return SQLResult(mRes, emptyMap())
    }
  }
}

private class SQLResult(val main: Rows, val rs: Map<String, Rows>): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw(): Rows {
    return main
  }
}


