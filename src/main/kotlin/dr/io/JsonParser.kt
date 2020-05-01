package dr.io

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

object JsonParser {
  val mapper: ObjectMapper = jacksonObjectMapper()
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

  fun write(value: Any): String {
    return mapper.writeValueAsString(value)
  }

  fun <T: Any> read(json: String, type: KClass<out T>): T {
    return mapper.readValue(json, type.java)
  }

  @Suppress("UNCHECKED_CAST")
  fun readParams(json: String): Map<String, Any> {
    return mapper.readValue(json, Map::class.java) as Map<String, Any>
  }
}