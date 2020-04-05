package dr.schema

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

enum class FieldType {
  TEXT, INT, FLOAT, BOOL,
  TIME, DATE, DATETIME
}

private val TEXT = typeOf<String?>()
private val INT = typeOf<Int?>()
private val LONG = typeOf<Long?>()
private val FLOAT = typeOf<Float?>()
private val DOUBLE = typeOf<Double?>()
private val BOOL = typeOf<Boolean?>()

private val TIME = typeOf<LocalTime?>()
private val DATE = typeOf<LocalDate?>()
private val DATETIME = typeOf<LocalDateTime?>()

object TypeEngine {
  val ID = LONG

  private val typeToClass = mutableMapOf<FieldType, KClass<*>>().apply {
    put(FieldType.TEXT, String::class)
    put(FieldType.INT, Long::class)
    put(FieldType.FLOAT, Double::class)
    put(FieldType.BOOL, Boolean::class)

    put(FieldType.TIME, LocalTime::class)
    put(FieldType.DATE, LocalDate::class)
    put(FieldType.DATETIME, LocalDateTime::class)
  }

  fun check(fType: FieldType, vType: KClass<Any>): Boolean {
    return typeToClass[fType] == vType
  }

  fun convert(type: KType): FieldType? {
    if (type.isSubtypeOf(TEXT)) return FieldType.TEXT
    if (type.isSubtypeOf(INT) || type.isSubtypeOf(LONG)) return FieldType.INT
    if (type.isSubtypeOf(FLOAT) || type.isSubtypeOf(DOUBLE)) return FieldType.FLOAT
    if (type.isSubtypeOf(BOOL)) return FieldType.BOOL

    if (type.isSubtypeOf(TIME)) return FieldType.TIME
    if (type.isSubtypeOf(DATE)) return FieldType.DATE
    if (type.isSubtypeOf(DATETIME)) return FieldType.DATETIME

    return null
  }
}