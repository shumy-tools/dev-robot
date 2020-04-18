package dr.io

import com.fasterxml.jackson.databind.node.ArrayNode
import dr.JsonParser
import dr.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

class InputProcessor(private val schema: Schema) {
  private val mapper = JsonParser.mapper

  fun create(type: SEntity, json: String) = create(type.clazz, json)

  fun create(value: Any): DEntity {
    val name = value.javaClass.kotlin.qualifiedName
    val sEntity = schema.masters[name] ?: throw Exception("Master entity nof found! - ($name)")

    return DEntity(sEntity, cEntity = value)
  }

  fun create(type: KClass<out Any>, json: String): DEntity {
    val sEntity = schema.find(type)
    if (sEntity.type != EntityType.MASTER)
      throw Exception("Creation is only valid for master entities! - (${sEntity.name})")

    val value = mapper.readValue(json, type.java)
    return DEntity(sEntity, cEntity = value)
  }

  fun update(type: KClass<out Any>, json: String) = update(schema.find(type), json)

  fun update(type: SEntity, map: Map<String, Any?>): DEntity {
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
          FieldType.INT -> value is Long
          FieldType.FLOAT -> value is Double
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

    return DEntity(type, mEntity = map)
  }

  fun update(type: SEntity, json: String): DEntity {
    val node = mapper.readTree(json)
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
          FieldType.INT -> vNode.asLong()
          FieldType.FLOAT -> vNode.asDouble()
          FieldType.BOOL -> vNode.asBoolean()
          FieldType.TIME -> LocalTime.parse(vNode.asText())
          FieldType.DATE -> LocalDate.parse(vNode.asText())
          FieldType.DATETIME -> LocalDateTime.parse(vNode.asText())
        }

        is SRelation -> when (sFieldOrRelation.type) {
          RelationType.CREATE -> {
            val cType = if (sFieldOrRelation.ref.isSealed) Pack::class.java else schema.findClass(sFieldOrRelation.ref.name).java
            if (!sFieldOrRelation.isCollection) mapper.treeToValue(vNode, cType) else (vNode as ArrayNode).map { mapper.treeToValue(it, cType) }
          }
          RelationType.LINK -> mapper.treeToValue(vNode, LinkData::class.java)
        }
      }
    }

    return DEntity(type, mEntity = map)
  }
}