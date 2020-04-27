package dr.adaptor

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dr.io.*
import dr.spi.IModificationAdaptor
import java.lang.Exception
import java.sql.SQLException
import java.sql.Statement

class SQLAdaptor(val url: String): IModificationAdaptor {
  private val config = HikariConfig().also {
    it.jdbcUrl = url
    it.addDataSourceProperty("cachePrepStmts", "true")
    it.addDataSourceProperty("prepStmtCacheSize", "250")
    it.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  }

  private val ds = HikariDataSource(config)

  fun createSchema(instructions: List<SchemaInstruction>) {
    val conn = ds.connection
    try {
      instructions.forEach { inst ->
        val sql = inst.createSql()
        println(sql)
        conn.createStatement().use { it.execute(sql) }
      }

      instructions.forEach { inst ->
        inst.refs.forEach { ref ->
          val sql = inst.foreignKeySql(ref)
          println(sql)
          conn.createStatement().use { it.execute(sql) }
        }
      }
    } catch (ex: Exception) {
      println(ex.message)
      throw ex
    } finally {
      conn.close()
    }
  }

  override fun commit(instructions: Instructions) {
    val conn = ds.connection
    try {
      println("TX-START")

      instructions.exec { inst ->
        //println("  INST - $inst")

        val table = inst.table.sqlName()
        val separator = if (inst.data.isNotEmpty() && inst.resolvedRefs.isNotEmpty()) "," else ""

        val sql = when (inst) {
          is Insert -> {
            val number = inst.data.size + inst.resolvedRefs.size
            val nInputs = (1..number).joinToString { "?" }

            val fields = inst.data.keys.joinToString { """"${it.name()}"""" }
            val refs = inst.resolvedRefs.keys.joinToString { """"${inst.table.refSqlName(it)}"""" }
            "INSERT INTO $table($fields$separator $refs) VALUES ($nInputs);"
          }

          is Update -> {
            val fields = inst.data.keys.joinToString { """"${it.name()}" = ?""" }
            val refs = inst.resolvedRefs.keys.joinToString { """"${inst.table.refSqlName(it)}" = ?""" }
            """UPDATE $table SET $fields$separator $refs WHERE "@id" = ${inst.id};"""
          }

          is Delete -> {
            ""
          }
        }

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
          throw SQLException("Instruction failed, no rows affected - $inst");

        val genKeys = stmt.generatedKeys
        val id = if (genKeys.next()) genKeys.getLong(1) else 0L
        if (id != 0L) println(" - @id=$id") else println()

        id
      }

      println("TX-COMMIT")
    } catch (ex: Exception) {
      println()
      println("  ERROR -> ${ex.message}")
      throw ex
    } finally {
      conn.close()
    }
  }
}


