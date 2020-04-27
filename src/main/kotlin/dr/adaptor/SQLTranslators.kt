package dr.adaptor

import dr.io.*
import dr.schema.FieldType

fun Table.sqlName(): String {
  val entity = sEntity.name.replace('.', '_')
  return if (sRelation == null) entity else "${entity}__${sRelation.name}"
}

fun Table.refSqlName(tRef: TRef) = when(tRef) {
  is TSuperRef -> "@super"

  is TDirectRef -> if (tRef.rel == null) "@ref" else "@ref__${tRef.rel.name}"

  is TInverseRef -> if (sRelation == null) {
    // direct refs without aux table
    if (tRef.rel == null) "@inv_${tRef.refTable.sqlName()}" else "@inv_${tRef.refTable.sqlName()}__${tRef.rel.name}"
  } else {
    // refs with aux table
    if (tRef.rel == null) "@inv" else "@inv__${tRef.rel.name}"
  }
}

fun SchemaInstruction.createSql(): String {
  val hasFields = if (props.isNotEmpty()) "," else ""
  val hasRefs = if (refs.isNotEmpty()) "," else ""

  return """
    |CREATE TABLE ${table.sqlName()} (
    |  "@id" IDENTITY NOT NULL PRIMARY KEY$hasFields
    |  ${props.joinToString(separator = ",\n  ") { it.tableColumn() }}$hasRefs
    |  ${refs.joinToString(separator = ",\n  ") { """"${table.refSqlName(it)}" BIGINT""" }}
    |);
  """.trimMargin()
}

fun SchemaInstruction.foreignKeySql(ref: TRef) = """
  |ALTER TABLE ${table.sqlName()}
  |  ADD FOREIGN KEY ("${table.refSqlName(ref)}")
  |  REFERENCES ${ref.refTable.sqlName()}("@id");
""".trimMargin()

fun TProperty.tableColumn() = when(this) {
  is TType -> """"${name()}" VARCHAR(255) NOT NULL"""

  is TEmbedded -> {
    val optional = if (!rel.isOptional || rel.isCollection) "NOT NULL" else ""
    """"${name()}" JSON $optional"""
  }

  is TField -> {
    val optional = if (!field.isOptional) "NOT NULL" else ""
    when (field.type) {
      FieldType.TEXT -> """"${name()}" VARCHAR(255) $optional"""
      FieldType.INT -> """"${name()}" INT $optional"""
      FieldType.LONG -> """"${name()}" BIGINT $optional"""
      FieldType.FLOAT -> """"${name()}" REAL $optional"""
      FieldType.DOUBLE -> """"${name()}" DOUBLE $optional"""
      FieldType.BOOL -> """"${name()}" BOOLEAN $optional"""
      FieldType.TIME -> """"${name()}" TIME $optional"""
      FieldType.DATE -> """"${name()}" DATE $optional"""
      FieldType.DATETIME -> """"${name()}" TIMESTAMP $optional"""
    }
  }
}