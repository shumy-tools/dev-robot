package dr.query

import dr.schema.FieldType
import dr.schema.SEntity
import dr.schema.SField
import dr.schema.tabular.ID
import dr.schema.tabular.TYPE
import dr.spi.IQueryExecutor
import dr.spi.IResult
import kotlin.reflect.KClass

/* ------------------------- internal api -------------------------*/
class Parameter(val entity: SEntity, val field: String, val comp: CompType, val param: QParam)

class QueryExecutorWithValidator(private val native: IQueryExecutor, parameters: List<Parameter>): IQueryExecutor {
  private val args: List<Parameter> = parameters.filter { it.param.type == ParamType.PARAM }

  init {
    // pre-check literal parameters
    parameters.filter { it.param.type != ParamType.PARAM }.forEach {
      if (!check(it.entity, it.field, it.comp, it.param.value))
        throw Exception("Invalid predicate '${it.entity.name}.${it.field} ${it.comp} ${it.param.value.javaClass.kotlin.qualifiedName}'")
    }
  }

  override fun exec(params: Map<String, Any>): IResult {
    args.forEach {
      val name = it.param.value as String
      val value = params[name] ?: throw Exception("Expected input value for: '$name'")

      if (!check(it.entity, it.field, it.comp, value))
        throw Exception("Invalid predicate '${it.entity.name}.${it.field} ${it.comp} ${value.javaClass.kotlin.qualifiedName}' for parameter '$name'")
    }

    return native.exec(params)
  }

  private fun check(entity: SEntity, field: String, comp: CompType, value: Any) = when (field) {
    // TODO: insert other special types
    ID -> fieldTable.check(FieldType.LONG, comp, value.javaClass.kotlin)
    TYPE -> fieldTable.check(FieldType.TEXT, comp, value.javaClass.kotlin)
    else -> {
      val eField = entity.fields[field] ?: (entity.rels[field] ?: throw Exception("Bug! Expected '${entity.name}.$field'"))
      if (eField is SField) {
        fieldTable.check(eField.type, comp, value.javaClass.kotlin)
      } else {
        // TODO: check relation constraints?
        false
      }
    }
  }
}

/* ------------------------- helpers -------------------------*/
private class CompatibilityTable {
  private val table: MutableMap<FieldType, MutableMap<CompType, KClass<*>>> = mutableMapOf()

  fun set(fieldType: FieldType, compType: CompType, argType: KClass<*>) {
    val second = table.getOrPut(fieldType) { mutableMapOf() }
    second[compType] = argType
  }

  fun check(fieldType: FieldType, compType: CompType, argType: KClass<*>): Boolean {
    return table[fieldType]?.get(compType) == argType
  }
}

// FieldType -> (TEXT, INT, FLOAT, BOOL, TIME, DATE, DATETIME)
// CompType -> (EQUAL, DIFFERENT, MORE, LESS, MORE_EQ, LESS_EQ, IN)

// TODO: incomplete table of constraints?
private val fieldTable = CompatibilityTable().apply {
  set(FieldType.TEXT, CompType.EQUAL, String::class)
  set(FieldType.TEXT, CompType.DIFFERENT, String::class)
  set(FieldType.TEXT, CompType.IN, List::class)

  set(FieldType.INT, CompType.EQUAL, Int::class)
  set(FieldType.LONG, CompType.EQUAL, Long::class)
}