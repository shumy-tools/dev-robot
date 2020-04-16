package dr.io

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class LinkData

  sealed class ManyLinks: LinkData()

    @JsonTypeName("many-links")
    data class ManyLinksWithoutTraits(val refs: Collection<Long>): ManyLinks() {
      constructor(vararg refs: Long): this(refs.toList())
    }

    @JsonTypeName("many-links-traits")
    data class ManyLinksWithTraits(val refs: Collection<Traits>): ManyLinks() {
      constructor(vararg refs: Traits): this(refs.toList())
    }

    @JsonTypeName("many-unlink")
    data class ManyUnlink(val refs: Collection<Long>): ManyLinks() {
      constructor(vararg refs: Long): this(refs.toList())
    }

  sealed class OneLink: LinkData()

    @JsonTypeName("one-link")
    data class OneLinkWithoutTraits(val ref: Long): OneLink()

    @JsonTypeName("one-link-traits")
    data class OneLinkWithTraits(val ref: Traits): OneLink()

    @JsonTypeName("one-unlink")
    data class OneUnlink(val ref: Long): OneLink()

class InputProcessor(val schema: Schema) {
  private val mapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  init {
    mapper.registerSubtypes(
      ManyLinksWithoutTraits::class.java,
      OneLinkWithoutTraits::class.java,
      ManyLinksWithTraits::class.java,
      OneLinkWithTraits::class.java,
      ManyUnlink::class.java,
      OneUnlink::class.java
    )
  }

  fun create(type: SEntity, json: String): DEntity {
    val value = mapper.readValue(json, type.clazz.java)
    return DEntity(type, cEntity = value)
  }

  fun create(type: KClass<out Any>, json: String): DEntity {
    val sEntity = schema.find(type)

    val value = mapper.readValue(json, type.java)
    return DEntity(sEntity, cEntity = value)
  }

  fun update(type: KClass<out Any>, json: String) = update(schema.find(type), json)

  fun update(type: SEntity, json: String): DEntity {
    val node = mapper.readTree(json)
    val map = linkedMapOf<String, Any?>()

    for (nName in node.fieldNames()) {
      val sFieldOrRelation = type.getFieldOrRelation(nName) ?: throw Exception("Property not found! -  - (${type.name}, ${nName})")
      if (!sFieldOrRelation.isInput)
        throw Exception("Invalid input field! - (${type.name}, ${nName})")

      val vNode = node[nName]
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

  // fun query
}