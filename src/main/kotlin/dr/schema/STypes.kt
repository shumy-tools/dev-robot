package dr.schema

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
const val PARENT = "@parent"
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