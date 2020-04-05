package dr.modification

import dr.DrServer
import dr.schema.EventType.*
import dr.schema.Schema
import dr.schema.TypeEngine

/* ------------------------- api -------------------------*/
class ModificationEngine {
  private val schema: Schema by lazy { DrServer.schema }

  fun create(id: Long, new: Any): Long {
    val name = new.javaClass.kotlin.qualifiedName
    val sEntity = schema.entities[name] ?: throw Exception("Entity type not found! - ($name)")
    sEntity.onCreate(STARTED, id, new)


    // TODO: check entity constraints

    sEntity.onCreate(VALIDATED, id, new)

    // TODO: adaptor commit

    sEntity.onCreate(COMMITED, id, new)
    return 0L
  }

  fun update(name: String, id: Long, data: Map<String, Any>) {
    val sEntity = schema.entities[name] ?: throw Exception("Entity type not found! - ($name)")
    sEntity.onUpdate(STARTED, id, data)

    // TODO: does id exist?

    for ((key, value) in data) {
      val field = sEntity.fields[key] ?: throw Exception("Entity field not found! - ($name, $key)")
      val type = value.javaClass.kotlin

      if (!field.isInput)
        throw Exception("Invalid input field! - ($name, $key)")

      if (!TypeEngine.check(field.type, type))
        throw Exception("Invalid field type! - ($name, $key) (expected ${field.type} found ${type.simpleName})")

      field.checks.forEach {
        it.check(value)?.let { msg ->
          throw Exception("Failed check constraint '$msg'! - ($name, $key)")
        }
      }
    }

    sEntity.onUpdate(VALIDATED, id, data)

    // TODO: adaptor commit

    sEntity.onUpdate(COMMITED, id, data)
  }

}