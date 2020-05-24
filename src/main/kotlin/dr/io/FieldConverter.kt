package dr.io

import dr.JsonParser
import dr.schema.FieldType
import dr.schema.JMap
import dr.schema.SPECIAL
import dr.schema.TRAITS
import dr.schema.tabular.*

// used to convert fields to DB and back
object FieldConverter {
  fun save(key: TProperty, value: Any) = when {
    key is TEmbedded -> JsonParser.write(value)
    value is JMap -> JsonParser.write(value)
    else -> value
  }

  fun load(tables: Tables, table: STable, name: String, value: Any) = if (name.startsWith(TRAITS)) {
    val traitSplit = name.substring(1).split(SPECIAL)
    val sTrait = tables.schema.traits.getValue(traitSplit.first())
    JsonParser.read((value as String), sTrait.clazz)
  } else {
    val sTable = if (table.sRelation == null) table else tables.get(table.sRelation.ref)
    val sProp = sTable.props[name]
    if (sProp is TField && sProp.field.type == FieldType.JMAP) {
      JsonParser.read((value as String), JMap::class)
    } else value
  }
}