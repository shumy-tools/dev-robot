package dr.io

import com.fasterxml.jackson.databind.node.ArrayNode
import dr.schema.*
import dr.state.Machine
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

class InputProcessor(private val schema: Schema, private val machines: Map<SEntity, Machine<*, *>>) {
  fun create(type: SEntity, json: String) = create(type.clazz, json)

  fun create(type: KClass<out Any>, json: String): DEntity {
    val entity = JsonParser.readJson(json, type)
    return create(entity)
  }

  fun create(value: Any): DEntity {
    val master = if (value is Pack<*>) value.head else value
    val name = master.javaClass.kotlin.qualifiedName
    val sEntity = schema.masters[name] ?: throw Exception("Master entity nof found! - ($name)")

    if (value !is Pack<*> && sEntity.isSealed)
      throw Exception("A sealed entity must be created via Pack<*>! - ($name)")

    val dEntity = DEntity(sEntity, cEntity = value)
    machines[sEntity]?.onCreate(dEntity) // no need to process Pack.tail (only a master has a state-machine)
    return dEntity
  }

  fun update(type: KClass<out Any>, id: Long, json: String) = update(schema.find(type), id, json)

  fun update(type: SEntity, id: Long, json: String): DEntity {
    val node = JsonParser.readTree(json)
    val map = linkedMapOf<String, Any?>()

    for (nName in node.fieldNames()) {
      val sFieldOrRelation = type.getFieldOrRelation(nName) ?: throw Exception("Property not found! - (${type.name}, ${nName})")
      if (!sFieldOrRelation.isInput)
        throw Exception("Invalid input field! - (${type.name}, ${nName})")

      val vNode = node[nName]
      if (vNode.isNull && !sFieldOrRelation.isOptional)
        throw Exception("Invalid 'null' input! - (${type.name}, ${nName})")

      map[nName] = when(sFieldOrRelation) {
        is SField -> when (sFieldOrRelation.type) {
          FieldType.TEXT -> vNode.asText()
          FieldType.INT -> vNode.asInt()
          FieldType.LONG -> vNode.asLong()
          FieldType.FLOAT -> vNode.asDouble().toFloat()
          FieldType.DOUBLE -> vNode.asDouble()
          FieldType.BOOL -> vNode.asBoolean()
          FieldType.TIME -> LocalTime.parse(vNode.asText())
          FieldType.DATE -> LocalDate.parse(vNode.asText())
          FieldType.DATETIME -> LocalDateTime.parse(vNode.asText())
        }

        is SRelation -> when (sFieldOrRelation.type) {
          RelationType.CREATE -> {
            val cType = if (sFieldOrRelation.ref.isSealed) Pack::class else schema.findClass(sFieldOrRelation.ref.name)
            if (!sFieldOrRelation.isCollection) JsonParser.readNode(vNode, cType) else (vNode as ArrayNode).map { JsonParser.readNode(it, cType) }
          }
          RelationType.LINK -> JsonParser.readNode(vNode, LinkData::class)
        }
      }
    }

    val dEntity = DEntity(type, mEntity = map)
    machines[type]?.onUpdate(id, dEntity)
    return dEntity
  }

  fun update(type: SEntity, id: Long, map: Map<String, Any?>): DEntity {
    for (nName in map.keys) {
      val sFieldOrRelation = type.getFieldOrRelation(nName) ?: throw Exception("Property not found! - (${type.name}, ${nName})")
      if (!sFieldOrRelation.isInput)
        throw Exception("Invalid input field! - (${type.name}, ${nName})")

      val value = map[nName]
      if (value == null && !sFieldOrRelation.isOptional)
        throw Exception("Invalid 'null' input! - (${type.name}, ${nName})")

      val isOk = when(sFieldOrRelation) {
        is SField -> when (sFieldOrRelation.type) {
          FieldType.TEXT -> value is String
          FieldType.INT -> value is Int
          FieldType.LONG -> value is Long
          FieldType.FLOAT -> value is Float
          FieldType.DOUBLE -> value is Double
          FieldType.BOOL -> value is Boolean
          FieldType.TIME -> value is LocalTime
          FieldType.DATE -> value is LocalDate
          FieldType.DATETIME -> value is LocalDateTime
        }

        is SRelation -> when (sFieldOrRelation.type) {
          RelationType.CREATE -> {
            val cType = if (sFieldOrRelation.ref.isSealed) Pack::class else schema.findClass(sFieldOrRelation.ref.name)
            value?.let { value.javaClass.kotlin.isSubclassOf(cType) } ?: true
          }
          RelationType.LINK -> value?.let { value.javaClass.kotlin.isSubclassOf(LinkData::class) } ?: true
        }
      }

      if (!isOk)
        throw Exception("Invalid input type! - (${type.name}, ${nName})")
    }

    val dEntity = DEntity(type, mEntity = map)
    machines[type]?.onUpdate(id, dEntity)
    return dEntity
  }
}