package dr.io

import dr.JsonParser
import dr.ctx.Context
import dr.schema.FieldType
import dr.schema.SPECIAL
import dr.schema.TRAITS
import dr.schema.tabular.STable
import dr.schema.tabular.TEmbedded
import dr.schema.tabular.TField
import dr.schema.tabular.TProperty

// used to convert fields to DB and back
object FieldConverter {
  fun save(key: TProperty, value: Any) = when {
    key is TEmbedded -> JsonParser.write(value)
    value is Map<*, *> -> JsonParser.write(value)
    value is List<*> -> JsonParser.write(value)
    value is Set<*> -> JsonParser.write(value)
    else -> value
  }

  fun load(table: STable, name: String, value: Any) = if (name.startsWith(TRAITS)) {
    val tables = Context.session.tables
    val traitSplit = name.substring(1).split(SPECIAL)
    val sTrait = tables.schema.traits.getValue(traitSplit.first())
    JsonParser.read((value as String), sTrait.clazz)
  } else {
    val tables = Context.session.tables
    val sTable = if (table.sRelation == null) table else tables.get(table.sRelation.ref)
    val sProp = sTable.props[name]
    if (sProp is TField) {
      when (sProp.field.type) {
        FieldType.MAP -> JsonParser.read((value as String), Map::class)
        FieldType.LIST -> JsonParser.read((value as String), List::class)
        FieldType.SET -> JsonParser.read((value as String), Set::class)
        else -> value
      }
    } else value
  }
}