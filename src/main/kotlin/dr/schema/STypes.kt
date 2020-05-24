package dr.schema

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonTypeInfo


const val TRAITS = "&"
const val SPECIAL = "@"

// keys
const val ID = "@id"
const val REF = "@ref"
const val INV = "@inv"

// fields
const val TYPE = "@type"
const val STATE = "@state"
const val OPEN = "@open"

// relations
const val SUPER = "@super"
const val HISTORY = "@history"

@FunctionalInterface
interface FieldCheck<T> {
  fun check(value: T): String?
}

class RefID(private var _id: Long? = null) {
  var onSet: ((Long) -> Unit)? = null

  var id = _id
    set(value) {
      field = value
      _id = value
      onSet?.invoke(value!!)
    }

  override fun toString() = "RefID(id=$_id)"
}

data class Pack<T: Any>(
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val head: T,

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val tail: List<Any>
) {
  constructor(head: T, vararg tail: Any): this(head, tail.toList())
}

data class Traits(
  val id: RefID,

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@type")
  val traits: List<Any>
) {
  constructor(id: RefID, vararg traits: Any): this(id, traits.toList())
}

class JMap {
  private val data = linkedMapOf<String, Any>()

  @JsonAnyGetter
  fun any(): Map<String, Any> = data

  operator fun get(key: String) = data[key]

  @JsonAnySetter
  operator fun set(key: String, value: Any?) {
    if (value == null) {
      data.remove(key)
      return
    }

    val type = TypeEngine.convert(value.javaClass.kotlin)
    if (type == null || type == FieldType.JMAP)
      throw Exception("Invalid type for a JProperty! - (${value.javaClass.simpleName})")

    data[key] = value
  }

  override fun toString() = data.toString()
}