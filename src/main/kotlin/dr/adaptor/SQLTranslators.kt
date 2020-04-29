package dr.adaptor

import dr.query.CompType.*
import dr.query.OperType.AND
import dr.query.OperType.OR
import dr.query.ParamType.PARAM
import dr.query.QExpression
import dr.query.QPredicate
import dr.query.QSelect
import dr.schema.FieldType
import dr.schema.tabular.*
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Select
import org.jooq.impl.DSL
import org.jooq.impl.DSL.param
import org.jooq.impl.DSL.table

/* ------------------------- schema -------------------------*/
const val SQL_ID = "@id"

fun Table.sqlName(): String {
  val entity = sEntity.name.replace('.', '_')
  return if (sRelation == null) entity else "${entity}__${sRelation.name}"
}

fun propSqlName(prop: TProperty) = when(prop) {
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

/* ------------------------- query -------------------------*/
fun fn(fName: String): Field<Any> = DSL.field(DSL.name(fName))

fun DSLContext.select(tlb: Table, selection: QSelect, filter: QExpression?, limit: Int?, page: Int?, includeIdFilter: Boolean = false): Select<*> {
  val allFields = if (selection.hasAll) {
    tlb.props.map { fn(propSqlName(it)) }
  } else {
    selection.fields.map { fn(it.name) }
  }

  val directRefs = tlb.refs.filterIsInstance<TDirectRef>().map { fn(tlb.refSqlName(it)) }
  val dbSelect = select(
    listOf(fn(SQL_ID))
      .plus(allFields)
      .plus(directRefs))
    .from(table(tlb.sqlName()))

  val dbFilter = mutableListOf<Condition>()
  if (filter != null) dbFilter.add(expression(tlb, filter))
  //if (includeIdFilter) dbFilter.add(fn(SQL_ID).`in`(idFields as java.util.Collection<*>))

  val dbFiltered = dbSelect.where(dbFilter)
  return when {
    page != null -> dbFiltered.limit(limit).offset(page * limit!!)
    limit != null -> dbFiltered.limit(limit)
    else -> dbFiltered
  }
}

fun DSLContext.expression(tlb: Table, expr: QExpression): Condition = if (expr.predicate != null) {
  filter(tlb, expr.predicate)
} else {
  val left = expression(tlb, expr.left!!)
  val right = expression(tlb, expr.right!!)

  when(expr.oper!!) {
    OR -> left.or(right)
    AND -> left.and(right)
  }
}

@Suppress("UNCHECKED_CAST")
fun DSLContext.filter(tlb: Table, predicate: QPredicate): Condition {
  // TODO: build a sub-query path!
  val subQuery = predicate.path.first().name

  //mapper.params[subQuery] = predicate.param

  val path = fn(subQuery)
  val value = if (predicate.param.type == PARAM) param(predicate.param.value as String) else predicate.param.value

  return when(predicate.comp) {
    EQUAL -> path.eq(value)
    DIFFERENT -> path.ne(value)
    MORE -> path.greaterThan(value)
    LESS -> path.lessThan(value)
    MORE_EQ -> path.greaterOrEqual(value)
    LESS_EQ -> path.lessOrEqual(value)
    IN -> path.`in`(value)
  }
}