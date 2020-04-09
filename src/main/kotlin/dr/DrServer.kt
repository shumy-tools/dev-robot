package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.action.ActionEngine
import dr.modification.*
import dr.notification.NotificationEngine
import dr.query.QueryEngine
import dr.schema.Schema
import kotlin.reflect.KClass

object DrServer {
  private val mapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

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

  var enabled = false
    private set

  lateinit var schema: Schema
  lateinit var tableTranslator: Map<String, String>

  lateinit var qEngine: QueryEngine
  lateinit var mEngine: ModificationEngine
  lateinit var aEngine: ActionEngine
  lateinit var nEngine: NotificationEngine

  fun start(port: Int) {
    tableTranslator = schema.entities.map { (name, _) ->
      name to name.replace('.', '_').toLowerCase()
    }.toMap()

    this.enabled = true
  }

  fun serialize(value: Any): String {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }

  fun <T : Any> deserialize(value: String, clazz: KClass<T>): T {
    return mapper.readValue(value, clazz.java)
  }
}

/*class ItemSerializer(clazz: Class<Traits>? = null): StdSerializer<Traits>(clazz) {
  override fun serialize(value: Traits, jgen: JsonGenerator, provider: SerializerProvider) {
    with (jgen) {
      writeStartObject()
        writeArrayFieldStart("traits")
        for (trait in value.traits) {
          writeStartObject()
            writeStringField("@type", trait.javaClass.canonicalName)
            //write
          writeEndObject()
        }
        writeEndArray()
      writeEndObject()
    }
  }
}*/