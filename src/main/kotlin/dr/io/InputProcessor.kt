package dr.io

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import dr.JsonParser
import dr.schema.*
import dr.schema.tabular.TYPE
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

private val nOneAdd = OneAdd::value.name
private val nManyAdd = ManyAdd::values.name
private val nOneRemove = OneRemove::ref.name
private val nManyRemove = ManyRemove::refs.name
private val nOneUnlink = OneUnlink::ref.name

class InputProcessor(val schema: Schema) {
  fun create(type: SEntity, json: String) = create(type.clazz, json)

  fun create(type: KClass<out Any>, json: String): DEntity {
    val entity = JsonParser.read(json, type)
    return create(entity)
  }

  fun create(value: Any): DEntity {
    val master = if (value is Pack<*>) value.head else value
    val name = master.javaClass.kotlin.qualifiedName
    val sEntity = schema.masters[name] ?: throw Exception("Master entity nof found! - ($name)")

    if (value !is Pack<*> && sEntity.isSealed)
      throw Exception("A sealed entity must be created via Pack<*>! - ($name)")

    return DEntity(RefID(), sEntity, cEntity = value)
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
          RelationType.OWN -> {
            val nType = vNode[TYPE]?.asText() ?: throw Exception("'$TYPE' field not found for '$nName'!")
            val cType = if (sFieldOrRelation.ref.isSealed) Pack::class else schema.findClass(sFieldOrRelation.ref.name)
            when (nType) {
              ONE_ADD -> {
                val nValue = vNode[nOneAdd] ?: throw Exception("'$nOneAdd' field not found for '$nName'!")
                OneAdd(nValue.convert(id, cType))
              }
              MANY_ADD -> {
                val nValue = vNode[nManyAdd] ?: throw Exception("'$nManyAdd' not found for '$nName'!")
                ManyAdd((nValue as ArrayNode).map { it.convert(id, cType) })
              }
              ONE_RMV -> {
                val nValue = vNode[nOneRemove]
                val value = if (nValue == null) RefID(null) else JsonParser.readNode(nValue, RefID::class)
                OneRemove(value)
              }
              MANY_RMV -> {
                val nValue = vNode[nManyRemove] ?: throw Exception("'$nManyRemove' not found for '$nName'!")
                ManyRemove((nValue as ArrayNode).map { JsonParser.readNode(it, RefID::class) })
              }
              else -> throw Exception("Unrecognized @type for for '$nName'!")
            }
          }
          RelationType.LINK -> {
            val nType = vNode[TYPE]?.asText() ?: throw Exception("'$TYPE' field not found for '$nName'!")
            val nValue = vNode[nOneUnlink]
            if (nType == ONE_UNLINK && nValue == null) OneUnlink(RefID(null)) else JsonParser.readNode(vNode, LinkData::class)
          }
        }
      }
    }

    return DEntity(RefID(id), type, mEntity = map)
  }

  fun update(type: SEntity, id: Long, map: Map<String, Any?>, ignoreInputConstraint: Boolean = false): DEntity {
    for (nName in map.keys) {
      val sFieldOrRelation = type.getFieldOrRelation(nName) ?: throw Exception("Property not found! - (${type.name}, ${nName})")
      if (!ignoreInputConstraint && !sFieldOrRelation.isInput)
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
          RelationType.OWN -> value?.let { value is OwnData } ?: false //TODO: check the DEntity type?
          RelationType.LINK -> value?.let { value is LinkData } ?: false
        }
      }

      if (!isOk)
        throw Exception("Invalid input type! - (${type.name}, ${nName})")
    }

    return DEntity(RefID(id), type, mEntity = map)
  }

  private fun JsonNode.convert(id: Long, cType: KClass<out Any>): DEntity {
    val value = JsonParser.readNode(this, cType)

    val head = if (value is Pack<*>) value.head else value
    val name = head.javaClass.kotlin.qualifiedName
    val sEntity = schema.entities[name] ?: throw Exception("Entity nof found! - ($name)")

    if (value !is Pack<*> && sEntity.isSealed)
      throw Exception("A sealed entity must be created via Pack<*>! - ($name)")

    return DEntity(RefID(id), sEntity, cEntity = value)
  }
}