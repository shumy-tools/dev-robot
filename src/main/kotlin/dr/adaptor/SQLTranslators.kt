package dr.adaptor

import dr.io.*
import dr.schema.FieldType


/* ------------------------- schema -------------------------*/
fun Table.sqlName(): String {
  val entity = sEntity.name.replace('.', '_')
  return if (sRelation == null) entity else "${entity}__${sRelation.name}"
}

fun Table.refSqlName(tRef: TRef) = when(tRef) {
  is TSuperRef -> "@super"

  is TDirectRef -> if (!tRef.includeRelName) "@ref" else "@ref__${tRef.rel.name}"

  is TInverseRef -> if (sRelation == null) {
    // direct refs without aux table
    if (!tRef.includeRelName) "@inv_${tRef.refTable.sqlName()}" else "@inv_${tRef.refTable.sqlName()}__${tRef.rel.name}"
  } else {
    // refs with aux table
    if (!tRef.includeRelName) "@inv" else "@inv__${tRef.rel.name}"
  }
}

fun SchemaInstruction.createSql(): String {
  val hasFields = if (props.isNotEmpty()) "," else ""
  val hasRefs = if (refs.isNotEmpty()) "," else ""

  return """
    |CREATE TABLE ${table.sqlName()} (
    |  "@id" IDENTITY NOT NULL PRIMARY KEY$hasFields
    |  ${props.joinToString(separator = ",\n  ") { table.tableColumn(it) }}$hasRefs
    |  ${refs.joinToString(separator = ",\n  ") { table.tableColumn(it) }}
    |);
  """.trimMargin()
}

fun SchemaInstruction.foreignKeySql(ref: TRef) = """
  |ALTER TABLE ${table.sqlName()}
  |  ADD FOREIGN KEY ("${table.refSqlName(ref)}")
  |  REFERENCES ${ref.refTable.sqlName()}("@id");
""".trimMargin()


fun Table.tableColumn(prop: TProperty) = when(prop) {
  is TType -> """"${prop.name()}" VARCHAR(255) NOT NULL"""

  is TEmbedded -> {
    val optional = if (!prop.rel.isOptional || prop.rel.isCollection) "NOT NULL" else ""
    val unique = if (prop.rel.isUnique) """, UNIQUE KEY "unique_${sqlName()}__${prop.name()}" ("${prop.name()}")""" else ""
    """"${prop.name()}" JSON $optional$unique"""
  }

  is TField -> {
    val optional = if (!prop.field.isOptional) "NOT NULL" else ""
    val unique = if (prop.field.isUnique) """, UNIQUE KEY "unique_${sqlName()}__${prop.name()}" ("${prop.name()}")""" else ""
    when (prop.field.type) {
      FieldType.TEXT -> """"${prop.name()}" VARCHAR(255) $optional$unique"""
      FieldType.INT -> """"${prop.name()}" INT $optional$unique"""
      FieldType.LONG -> """"${prop.name()}" BIGINT $optional$unique"""
      FieldType.FLOAT -> """"${prop.name()}" REAL $optional$unique"""
      FieldType.DOUBLE -> """"${prop.name()}" DOUBLE $optional$unique"""
      FieldType.BOOL -> """"${prop.name()}" BOOLEAN $optional$unique"""
      FieldType.TIME -> """"${prop.name()}" TIME $optional$unique"""
      FieldType.DATE -> """"${prop.name()}" DATE $optional$unique"""
      FieldType.DATETIME -> """"${prop.name()}" TIMESTAMP $optional$unique"""
    }
  }
}

fun Table.tableColumn(ref: TRef): String {
  val unique = if (ref.isUnique()) """, UNIQUE KEY "unique_${sqlName()}__${refSqlName(ref)}" ("${refSqlName(ref)}")""" else ""
  return """"${refSqlName(ref)}" BIGINT$unique"""
}


/* ------------------------- data -------------------------*/
fun Instruction.sql(): String {
  val tableName = table.sqlName()
  val separator = if (data.isNotEmpty() && resolvedRefs.isNotEmpty()) "," else ""

  return when (this) {
    is Insert -> {
      val number = data.size + resolvedRefs.size
      val nInputs = (1..number).joinToString { "?" }

      val fields = data.keys.joinToString { """"${it.name()}"""" }
      val refs = resolvedRefs.keys.joinToString { """"${table.refSqlName(it)}"""" }
      "INSERT INTO $tableName($fields$separator $refs) VALUES ($nInputs);"
    }

    is Update -> {
      val fields = data.keys.joinToString { """"${it.name()}" = ?""" }
      val refs = resolvedRefs.keys.joinToString { """"${table.refSqlName(it)}" = ?""" }
      """UPDATE $tableName SET $fields$separator $refs WHERE "@id" = ${id};"""
    }

    is Delete -> {
      // TODO: disable stuff?
      ""
    }
  }
}