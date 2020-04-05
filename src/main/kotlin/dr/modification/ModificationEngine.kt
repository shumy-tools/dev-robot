package dr.modification

import dr.DrServer
import dr.schema.EventType.*
import dr.schema.FieldType
import dr.schema.RelationType
import dr.schema.Schema
import dr.schema.TypeEngine
import kotlin.reflect.full.isSubclassOf

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

  fun update(name: String, id: Long, tree: Map<String, Any>) {
    val sEntity = schema.entities[name] ?: throw Exception("Entity type not found! - ($name)")
    sEntity.onUpdate(STARTED, id, tree)

    // TODO: does id exist?

    var lEntity = sEntity
    for ((key, value) in tree) {
      val vType = value.javaClass.kotlin

      val field = lEntity.fields[key]
      val rel = lEntity.rels[key]
      when {
        field != null -> {
          if (!TypeEngine.check(field.type, vType))
            throw Exception("Invalid field type! - ($name, $key) (expected ${field.type} found ${vType.simpleName})")
          // TODO: check field constraints
        }

        rel != null -> {
          if (rel.isCollection) {
            if (!vType.isSubclassOf(List::class))
              throw Exception("Invalid relation type! - ($name, $key) (expected ${List::class.simpleName} found ${vType.simpleName})")

            (value as List<Any>).forEach {
              val itType = it.javaClass.kotlin
              when(rel.type) {
                RelationType.CREATE -> if (!itType.isSubclassOf(Map::class))
                  throw Exception("Invalid relation item-type! - ($name, $key) (expected ${Map::class.simpleName} found ${itType.simpleName})")
                RelationType.LINK -> if(!TypeEngine.check(FieldType.INT, itType))
                  throw Exception("Invalid relation item-type! - ($name, $key) (expected ${FieldType.INT} found ${itType.simpleName})")
              }
            }
          } else {

          }

          // TODO: check relation constraints
        }

        else -> throw Exception("Field/Relation not found! - ($name, $key)")
      }
    }

    sEntity.onUpdate(VALIDATED, id, tree)

    // TODO: adaptor commit

    sEntity.onUpdate(COMMITED, id, tree)
  }

}