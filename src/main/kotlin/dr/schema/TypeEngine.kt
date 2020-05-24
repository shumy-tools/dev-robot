package dr.schema

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

enum class FieldType {
  TEXT, INT, LONG, FLOAT, DOUBLE, BOOL,
  TIME, DATE, DATETIME, JMAP
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
private val JMAP = typeOf<JMap?>()

object TypeEngine {
  val REFID_NULL = typeOf<RefID?>()

  val PACK = typeOf<Pack<*>>()
  val PACK_NULL = typeOf<Pack<*>?>()

  val LIST = typeOf<List<*>>()
  val LIST_REFID = typeOf<List<RefID>>()

  val TRAITS_NULL = typeOf<Traits?>()
  val LIST_TRAITS = typeOf<List<Traits>>()

  private val typeToClass = mutableMapOf<FieldType, KClass<*>>().apply {
    put(FieldType.TEXT, String::class)
    put(FieldType.INT, Int::class)
    put(FieldType.LONG, Long::class)
    put(FieldType.FLOAT, Float::class)
    put(FieldType.DOUBLE, Double::class)
    put(FieldType.BOOL, Boolean::class)

    put(FieldType.TIME, LocalTime::class)
    put(FieldType.DATE, LocalDate::class)
    put(FieldType.DATETIME, LocalDateTime::class)
    put(FieldType.JMAP, JMap::class)
  }

  fun check(fType: FieldType, vType: KClass<Any>): Boolean {
    return typeToClass[fType] == vType
  }

  fun convert(type: KType): FieldType? {
    if (type.isSubtypeOf(TEXT)) return FieldType.TEXT
    if (type.isSubtypeOf(INT)) return FieldType.INT
    if (type.isSubtypeOf(LONG)) return FieldType.LONG
    if (type.isSubtypeOf(FLOAT)) return FieldType.FLOAT
    if (type.isSubtypeOf(DOUBLE)) return FieldType.DOUBLE
    if (type.isSubtypeOf(BOOL)) return FieldType.BOOL

    if (type.isSubtypeOf(TIME)) return FieldType.TIME
    if (type.isSubtypeOf(DATE)) return FieldType.DATE
    if (type.isSubtypeOf(DATETIME)) return FieldType.DATETIME
    if (type.isSubtypeOf(JMAP)) return FieldType.JMAP

    return null
  }

  fun convert(type: KClass<*>) = convert(type.createType())
}