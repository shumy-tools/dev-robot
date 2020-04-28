package dr.adaptor

import dr.io.*
import dr.query.*
import dr.query.OperType.*
import dr.query.CompType.*
import dr.query.ParamType.*
import dr.schema.*
import dr.schema.tabular.*

/* ------------------------- schema -------------------------*/
const val SQL_ID = "@id"

fun Table.sqlName(): String {
  val entity = sEntity.name.replace('.', '_')
  return if (sRelation == null) entity else "${entity}__${sRelation.name}"
}

fun Table.propSqlName(prop: TProperty) = when(prop) {
  is TType -> "@type"
  is TEmbedded -> "@${prop.rel.name}"
  is TField -> prop.field.name
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

fun Table.createSql(): String {
  val hasFields = if (props.isNotEmpty()) "," else ""
  val hasRefs = if (refs.isNotEmpty()) "," else ""

  return """
    |CREATE TABLE ${sqlName()} (
    |  "$SQL_ID" IDENTITY NOT NULL PRIMARY KEY$hasFields
    |  ${props.joinToString(separator = ",\n  ") { propColumn(it) }}$hasRefs
    |  ${refs.joinToString(separator = ",\n  ") { refColumn(it) }}
    |);
  """.trimMargin()
}

fun Table.foreignKeySql(ref: TRef) = """
  |ALTER TABLE ${sqlName()}
  |  ADD FOREIGN KEY ("${refSqlName(ref)}")
  |  REFERENCES ${ref.refTable.sqlName()}("$SQL_ID");
""".trimMargin()


fun Table.propColumn(prop: TProperty) = when(prop) {
  is TType -> """"${propSqlName(prop)}" VARCHAR(255) NOT NULL"""

  is TEmbedded -> {
    val optional = if (!prop.rel.isOptional || prop.rel.isCollection) "NOT NULL" else ""
    val unique = if (prop.rel.isUnique) """, UNIQUE KEY "unique_${sqlName()}__${propSqlName(prop)}" ("${propSqlName(prop)}")""" else ""
    """"${propSqlName(prop)}" JSON $optional$unique"""
  }

  is TField -> {
    val optional = if (!prop.field.isOptional) "NOT NULL" else ""
    val unique = if (prop.field.isUnique) """, UNIQUE KEY "unique_${sqlName()}__${propSqlName(prop)}" ("${propSqlName(prop)}")""" else ""
    when (prop.field.type) {
      FieldType.TEXT -> """"${propSqlName(prop)}" VARCHAR(255) $optional$unique"""
      FieldType.INT -> """"${propSqlName(prop)}" INT $optional$unique"""
      FieldType.LONG -> """"${propSqlName(prop)}" BIGINT $optional$unique"""
      FieldType.FLOAT -> """"${propSqlName(prop)}" REAL $optional$unique"""
      FieldType.DOUBLE -> """"${propSqlName(prop)}" DOUBLE $optional$unique"""
      FieldType.BOOL -> """"${propSqlName(prop)}" BOOLEAN $optional$unique"""
      FieldType.TIME -> """"${propSqlName(prop)}" TIME $optional$unique"""
      FieldType.DATE -> """"${propSqlName(prop)}" DATE $optional$unique"""
      FieldType.DATETIME -> """"${propSqlName(prop)}" TIMESTAMP $optional$unique"""
    }
  }
}

fun Table.refColumn(ref: TRef): String {
  val unique = if (ref.isUnique) """, UNIQUE KEY "unique_${sqlName()}__${refSqlName(ref)}" ("${refSqlName(ref)}")""" else ""
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

      val fields = data.keys.joinToString { """"${table.propSqlName(it)}"""" }
      val refs = resolvedRefs.keys.joinToString { """"${table.refSqlName(it)}"""" }
      "INSERT INTO $tableName($fields$separator $refs) VALUES ($nInputs);"
    }

    is Update -> {
      val fields = data.keys.joinToString { """"${table.propSqlName(it)}" = ?""" }
      val refs = resolvedRefs.keys.joinToString { """"${table.refSqlName(it)}" = ?""" }
      """UPDATE $tableName SET $fields$separator $refs WHERE "$SQL_ID" = ${id};"""
    }

    is Delete -> {
      // TODO: disable stuff?
      ""
    }
  }
}


/* ------------------------- query -------------------------*/
class QueryMapper(val schema: SEntity) {
  lateinit var sql: String
  val params = linkedMapOf<String, QParam>()
}

fun testQuery(): String = "SELECT 1"

fun Table.select(selection: QSelect, filter: QExpression?, limit: Int?, page: Int?): QueryMapper {
  val mapper = QueryMapper(sEntity)

  val selectionSQL = if (selection.hasAll) {
    props.joinToString { """"${propSqlName(it)}"""" }
  } else {
    selection.fields.joinToString { """"${it.name}"""" }
  }

  val filterSQL = if (filter != null) "WHERE ${expression(mapper, filter)}" else ""

  val limitSQL = if (limit != null) " LIMIT $limit" else ""
  val limitAndPageSQL = if (page != null) "$limitSQL OFFSET ${page * limit!!}" else limitSQL

  mapper.sql = """SELECT "$SQL_ID", $selectionSQL FROM ${sqlName()} $filterSQL$limitAndPageSQL;"""
  return mapper
}

fun Table.expression(mapper: QueryMapper, expr: QExpression): String = if (expr.predicate != null) {
  filter(mapper, expr.predicate)
} else {
  "(${expression(mapper, expr.left!!)} ${expr.oper!!.sql()} ${expression(mapper, expr.right!!)})"
}

@Suppress("UNCHECKED_CAST")
fun Table.filter(mapper: QueryMapper, predicate: QPredicate): String {
  // TODO: build a sub-query path!
  val subQuery = predicate.path.first().name

  mapper.params[subQuery] = predicate.param

  return """"$subQuery" ${predicate.comp.sql()} ?"""
}

fun OperType.sql() = when(this) {
  OR -> "OR"
  AND -> "AND"
}

fun CompType.sql() = when(this) {
  EQUAL -> "="
  DIFFERENT -> "!="
  MORE -> ">"
  LESS -> "<"
  MORE_EQ -> ">="
  LESS_EQ -> "<="
  IN -> "IN"
}