package dr.query

import dr.schema.FieldType
import dr.schema.SField
import dr.schema.tabular.ID
import dr.schema.tabular.STable
import dr.schema.tabular.TYPE
import dr.spi.IQueryExecutor
import dr.spi.IResult
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/* ------------------------- internal api -------------------------*/
class Parameter(val table: STable, val field: String, val comp: CompType, val param: QParam)

class QueryExecutorWithValidator(private val native: IQueryExecutor<Any>, parameters: List<Parameter>): IQueryExecutor<Any> {
  private val args: List<Parameter> = parameters.filter { it.param.type == ParamType.PARAM }
  private val lpArgs: List<Parameter> = parameters.filter { it.param.type == ParamType.LP_PARAM }

  init {
    // pre-check literal parameters
    parameters.filter { it.param.type != ParamType.PARAM && it.param.type != ParamType.LP_PARAM }.forEach {
      if (!check(it.table, it.field, it.comp, it.param.value))
        throw Exception("Invalid predicate '${it.table.name}.${it.field} ${it.comp} ${it.param.value.javaClass.kotlin.qualifiedName}'")
    }
  }

  override fun exec(params: Map<String, Any>): IResult<Any> {
    lpArgs.forEach {
      val name = it.param.value as String
      val value = params[name] ?: throw Exception("Expected input value for: '$name'")

      if (value is Int) {
        if (value < 1)
          throw Exception("'limit' and 'page' must be > 0")
      } else
        throw Exception("Invalid type '${value.javaClass.kotlin.simpleName}' for parameter '$name'")
    }

    args.forEach {
      val name = it.param.value as String
      val value = params[name] ?: throw Exception("Expected input value for: '$name'")

      if (!check(it.table, it.field, it.comp, value))
        throw Exception("Invalid predicate '${it.table.name}.${it.field} ${it.comp} ${value.javaClass.kotlin.simpleName}' for parameter '$name'")
    }

    return native.exec(params)
  }

  private fun check(table: STable, field: String, comp: CompType, value: Any) = when (field) {
    // TODO: insert other special types
    ID -> fieldTable.check(FieldType.LONG, comp, value)
    TYPE -> fieldTable.check(FieldType.TEXT, comp, value)
    else -> {
      val eField = table.sEntity.fields[field] ?: (table.sEntity.rels[field] ?: throw Exception("Bug! Expected '${table.name}.$field'"))
      if (eField is SField) {
        fieldTable.check(eField.type, comp, value)
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

  fun check(fieldType: FieldType, compType: CompType, value: Any): Boolean {
    val argType = when (value) {
      is Int -> Long::class
      is Float -> Double::class
      else -> value.javaClass.kotlin
    }

    val testType = table[fieldType]?.get(compType)
    return if (testType != null) argType.isSubclassOf(testType) else false
  }
}

// (TEXT, INT, LONG, FLOAT, DOUBLE, BOOL, TIME, DATE, DATETIME)
// ('==' | '!=' | '>' | '<' | '>=' | '<=' | 'in')
private val fieldTable = CompatibilityTable().apply {
  // ==
  set(FieldType.TEXT, CompType.EQUAL, String::class)
  set(FieldType.INT, CompType.EQUAL, Long::class)
  set(FieldType.LONG, CompType.EQUAL, Long::class)
  set(FieldType.FLOAT, CompType.EQUAL, Double::class)
  set(FieldType.DOUBLE, CompType.EQUAL, Double::class)
  set(FieldType.BOOL, CompType.EQUAL, Boolean::class)
  set(FieldType.TIME, CompType.EQUAL, LocalTime::class)
  set(FieldType.DATE, CompType.EQUAL, LocalDate::class)
  set(FieldType.DATETIME, CompType.EQUAL, LocalDateTime::class)

  // !=
  set(FieldType.TEXT, CompType.DIFFERENT, String::class)
  set(FieldType.INT, CompType.DIFFERENT, Long::class)
  set(FieldType.LONG, CompType.DIFFERENT, Long::class)
  set(FieldType.FLOAT, CompType.DIFFERENT, Double::class)
  set(FieldType.DOUBLE, CompType.DIFFERENT, Double::class)
  set(FieldType.BOOL, CompType.DIFFERENT, Boolean::class)
  set(FieldType.TIME, CompType.DIFFERENT, LocalTime::class)
  set(FieldType.DATE, CompType.DIFFERENT, LocalDate::class)
  set(FieldType.DATETIME, CompType.DIFFERENT, LocalDateTime::class)

  // >
  set(FieldType.INT, CompType.MORE, Long::class)
  set(FieldType.LONG, CompType.MORE, Long::class)
  set(FieldType.FLOAT, CompType.MORE, Double::class)
  set(FieldType.DOUBLE, CompType.MORE, Double::class)
  set(FieldType.TIME, CompType.MORE, LocalTime::class)
  set(FieldType.DATE, CompType.MORE, LocalDate::class)
  set(FieldType.DATETIME, CompType.MORE, LocalDateTime::class)

  // <
  set(FieldType.INT, CompType.LESS, Long::class)
  set(FieldType.LONG, CompType.LESS, Long::class)
  set(FieldType.FLOAT, CompType.LESS, Double::class)
  set(FieldType.DOUBLE, CompType.LESS, Double::class)
  set(FieldType.TIME, CompType.LESS, LocalTime::class)
  set(FieldType.DATE, CompType.LESS, LocalDate::class)
  set(FieldType.DATETIME, CompType.LESS, LocalDateTime::class)

  // >=
  set(FieldType.INT, CompType.MORE_EQ, Long::class)
  set(FieldType.LONG, CompType.MORE_EQ, Long::class)
  set(FieldType.FLOAT, CompType.MORE_EQ, Double::class)
  set(FieldType.DOUBLE, CompType.MORE_EQ, Double::class)
  set(FieldType.TIME, CompType.MORE_EQ, LocalTime::class)
  set(FieldType.DATE, CompType.MORE_EQ, LocalDate::class)
  set(FieldType.DATETIME, CompType.MORE_EQ, LocalDateTime::class)

  // <=
  set(FieldType.INT, CompType.LESS_EQ, Long::class)
  set(FieldType.LONG, CompType.LESS_EQ, Long::class)
  set(FieldType.FLOAT, CompType.LESS_EQ, Double::class)
  set(FieldType.DOUBLE, CompType.LESS_EQ, Double::class)
  set(FieldType.TIME, CompType.LESS_EQ, LocalTime::class)
  set(FieldType.DATE, CompType.LESS_EQ, LocalDate::class)
  set(FieldType.DATETIME, CompType.LESS_EQ, LocalDateTime::class)

  // in
  set(FieldType.TEXT, CompType.IN, List::class)
  set(FieldType.INT, CompType.IN, List::class)
  set(FieldType.LONG, CompType.IN, List::class)
  set(FieldType.FLOAT, CompType.IN, List::class)
  set(FieldType.DOUBLE, CompType.IN, List::class)
  set(FieldType.TIME, CompType.IN, LocalTime::class)
  set(FieldType.DATE, CompType.IN, LocalDate::class)
  set(FieldType.DATETIME, CompType.IN, LocalDateTime::class)
}