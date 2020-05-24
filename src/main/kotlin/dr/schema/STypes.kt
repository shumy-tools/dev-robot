package dr.schema

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
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
  private var data: MutableMap<String, Any> = linkedMapOf()

  @JsonIgnore
  fun isEmpty() = data.isEmpty()

  @JsonAnyGetter
  fun any(): Map<String, Any> = data

  fun getOrPut(key: String): JMap {
    val value = data[key]
    if (value != null)
      return value as JMap

    val jmap = JMap()
    data[key] = jmap
    return jmap
  }

  @Suppress("UNCHECKED_CAST")
  operator fun get(key: String): Any? {
    val value = data[key]
    return if (value is Map<*, *>) JMap().also { it.data = (value as Map<String, Any>).toMutableMap() } else value
  }

  @JsonAnySetter
  operator fun set(key: String, value: Any?) {
    if (value == null) {
      data.remove(key)
      return
    }

    val nValue = if (value is Map<*, *>) {
      val jmap = JMap()
      value.forEach { jmap[it.key as String] = it.value }
      jmap
    } else value

    TypeEngine.convert(nValue.javaClass.kotlin) ?: throw Exception("Invalid type for a JProperty! - (${nValue.javaClass.simpleName})")
    data[key] = nValue
  }

  override fun toString() = data.toString()
}