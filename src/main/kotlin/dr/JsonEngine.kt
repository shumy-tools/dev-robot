package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.modification.*
import dr.schema.FieldType.*
import dr.schema.RelationType.CREATE
import dr.schema.RelationType.LINK
import dr.schema.SEntity
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

object JsonEngine {
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
      ManyLinkDelete::class.java,
      OneLinkDelete::class.java
    )
  }

  fun prettyPrint(value: Any): String {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }

  fun serialize(value: Any): String {
    return mapper.writeValueAsString(value)
  }

  fun <T : Any> deserialize(json: String, clazz: KClass<T>): T {
    return mapper.readValue(json, clazz.java)
  }

  fun deserialize(json: String, sEntity: SEntity): Map<String, Any?> {
    val node = mapper.readTree(json)
    val map = linkedMapOf<String, Any?>()

    // process fields
    for ((name, field) in sEntity.fields) {
      val value = node[name] ?: continue // ignore non existent
      map[name] = when (field.type) {
        TEXT -> value.asText()
        INT -> value.asLong()
        FLOAT -> value.asDouble()
        BOOL -> value.asBoolean()
        TIME -> LocalTime.parse(value.asText())
        DATE -> LocalDate.parse(value.asText())
        DATETIME -> LocalDateTime.parse(value.asText())
      }
    }

    // process relations
    for ((name, rel) in sEntity.rels) {
      val value = node[name] ?: continue // ignore non existent
      map[name] = when (rel.type) {
        CREATE -> if (!rel.isCollection) {
          mapper.treeToValue(value, Class.forName(rel.ref.name))
        } else {
          val type = Class.forName(rel.ref.name)
          val array = value as ArrayNode
          array.map { mapper.treeToValue(it, type) }
        }

        LINK -> mapper.treeToValue(value, LinkData::class.java)
      }
    }

    return map
  }
}