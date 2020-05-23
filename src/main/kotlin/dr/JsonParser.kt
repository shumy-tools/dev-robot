package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.io.*
import kotlin.reflect.KClass

object JsonParser {
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
      OneUnlink::class.java,
      ManyAdd::class.java,
      OneAdd::class.java,
      ManyRemove::class.java,
      OneRemove::class.java
    )
  }

  fun write(value: Any): String = mapper.writeValueAsString(value)
  fun <T: Any> read(json: String, type: KClass<out T>) = mapper.readValue(json, type.java)

  fun readTree(json: String): JsonNode = mapper.readTree(json)
  fun <T: Any> readNode(node: TreeNode, type: KClass<out T>): T = mapper.treeToValue(node, type.java)

  @Suppress("UNCHECKED_CAST")
  fun readMap(json: String): Map<String, Any> = mapper.readValue(json, Map::class.java) as Map<String, Any>
}