package dr.io

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class LinkData

  sealed class ManyLinks(): LinkData()

    @JsonTypeName("many-links")
    class ManyLinksWithoutTraits(val refs: Collection<Long>): ManyLinks()

    @JsonTypeName("many-links-traits")
    class ManyLinksWithTraits(val refs: Collection<Traits>): ManyLinks()

    @JsonTypeName("many-unlink")
    class ManyUnlink(val refs: Collection<Long>): ManyLinks()

  sealed class OneLink(): LinkData()

    @JsonTypeName("one-link")
    class OneLinkWithoutTraits(val ref: Long): OneLink()

    @JsonTypeName("one-link-traits")
    class OneLinkWithTraits(val ref: Traits): OneLink()

    @JsonTypeName("one-unlink")
    class OneUnlink(val ref: Long): OneLink()

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

  fun create(type: KClass<out Any>, json: String): DEntity {
    val sEntity = schema.find(type)
    val value = mapper.readValue(json, type.java)

    return DEntity(sEntity, cEntity = value)
  }

  fun update(type: KClass<out Any>, json: String) = update(schema.find(type), json)

  fun update(type: SEntity, json: String): DEntity {
    val node = mapper.readTree(json)
    val map = linkedMapOf<String, Any?>()

    // process fields
    for ((name, field) in type.fields) {
      val vNode = node[name] ?: continue // ignore non existent
      map[name] = when (field.type) {
        FieldType.TEXT -> vNode.asText()
        FieldType.INT -> vNode.asLong()
        FieldType.FLOAT -> vNode.asDouble()
        FieldType.BOOL -> vNode.asBoolean()
        FieldType.TIME -> LocalTime.parse(vNode.asText())
        FieldType.DATE -> LocalDate.parse(vNode.asText())
        FieldType.DATETIME -> LocalDateTime.parse(vNode.asText())
      }
    }

    // process relations
    for ((name, rel) in type.rels) {
      val vNode = node[name] ?: continue // ignore non existent
      map[name] = when (rel.type) {

        RelationType.CREATE -> {
          val cType = if (rel.ref.isSealed) Pack::class.java else Class.forName(rel.ref.name)
          if (!rel.isCollection) mapper.treeToValue(vNode, cType) else (vNode as ArrayNode).map { mapper.treeToValue(it, cType) }
        }

        RelationType.LINK -> mapper.treeToValue(vNode, LinkData::class.java)
      }
    }

    return DEntity(type, mEntity = map)
  }

  // fun query
}