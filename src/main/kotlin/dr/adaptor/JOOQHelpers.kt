package dr.adaptor

import dr.query.QField
import dr.query.QSelect
import dr.schema.FieldType
import dr.schema.SEntity
import dr.schema.SField
import dr.schema.tabular.*
import org.jooq.Field
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

const val MAIN = "main"

fun SEntity.sqlName() = name.replace('.', '_')

fun Table.sqlName(): String {
  val entity = sEntity.sqlName()
  return if (sRelation == null) entity else "${entity}__${sRelation.name}"
}

fun Table.refSqlName(tRef: TRef) = when(tRef) {
  is TSuperRef -> SUPER
  is TDirectRef -> if (!tRef.includeRelName) "@ref" else "@ref__${tRef.rel.name}"
  is TInverseRef -> if (sRelation == null) {
    // direct refs without aux table
    if (!tRef.includeRelName) "@inv_${tRef.refTable.sqlName()}" else "@inv_${tRef.refTable.sqlName()}__${tRef.rel.name}"
  } else {
    // refs with aux table
    if (!tRef.includeRelName) "@inv" else "@inv__${tRef.rel.name}"
  }
}

fun Table.dbFields(selection: QSelect, prefix: String = MAIN) = if (selection.hasAll) {
  props.filter { it.name != ID }.map { it.fn(prefix) }
} else {
  selection.fields.filter { it.name != ID }.map { it.fn(prefix) }
}

fun Table.dbDirectRefs(selection: QSelect) = selection.relations.mapNotNull {
  val value = directRefs[it.name]
  if (value != null) it to value else null
}.toMap()

// "PLATFORM_CLASS_MAPPED_TO_KOTLIN"
@Suppress("UNCHECKED_CAST")
fun idFn(prefix: String? = null): Field<Long> = if (prefix != null) {
  DSL.field(DSL.name(prefix, ID), java.lang.Long::class.java) as Field<Long>
} else {
  DSL.field(DSL.name(ID), java.lang.Long::class.java) as Field<Long>
}

@Suppress("UNCHECKED_CAST")
fun TRef.fn(tlb: Table, prefix: String? = null): Field<Long> = if (prefix != null) {
  DSL.field(DSL.name(prefix, tlb.refSqlName(this)), java.lang.Long::class.java) as Field<Long>
} else {
  DSL.field(DSL.name(tlb.refSqlName(this)), java.lang.Long::class.java) as Field<Long>
}

@Suppress("UNCHECKED_CAST")
fun TProperty.fn(prefix: String? = null): Field<Any> = if (prefix != null) {
  DSL.field(DSL.name(prefix, name), jType) as Field<Any>
} else {
  DSL.field(DSL.name(name), jType) as Field<Any>
}

@Suppress("UNCHECKED_CAST")
fun SField.fn(prefix: String? = null): Field<Any> = if (prefix != null) {
  DSL.field(DSL.name(prefix, name), jType) as Field<Any>
} else {
  DSL.field(DSL.name(name), jType) as Field<Any>
}

@Suppress("UNCHECKED_CAST")
fun QField.fn(prefix: String? = null): Field<Any> = if (prefix != null) {
  DSL.field(DSL.name(prefix, name), jType) as Field<Any>
} else {
  DSL.field(DSL.name(name), jType) as Field<Any>
}

fun FieldType.toSqlType() = when (this) {
  FieldType.TEXT -> SQLDataType.VARCHAR
  FieldType.INT -> SQLDataType.INTEGER
  FieldType.LONG -> SQLDataType.BIGINT
  FieldType.FLOAT -> SQLDataType.FLOAT
  FieldType.DOUBLE -> SQLDataType.DOUBLE
  FieldType.BOOL -> SQLDataType.BOOLEAN
  FieldType.TIME -> SQLDataType.LOCALTIME
  FieldType.DATE -> SQLDataType.LOCALDATE
  FieldType.DATETIME -> SQLDataType.LOCALDATETIME
}