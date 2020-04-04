package dr.schema

import kotlin.reflect.KClass

/* ------------------------- annotations -------------------------*/
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Listeners(vararg val value: KClass<out Any>)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Events(vararg val value: EventType)

/* ------------------------- enums -------------------------*/
enum class EventType {
  STARTED, VALIDATED, COMMITED
}

/* ------------------------- api -------------------------*/
open class EListener<T>() {
  open fun onRead(id: Long, data: Map<String, Any>) {}

  open fun onCreate(type: EventType, id: Long, new: T) {}
  open fun onUpdate(type: EventType, id: Long, data: Map<String, Any>) {}
  open fun onDelete(type: EventType, id: Long) {}

  open fun onAddCreate(type: EventType, id: Long, field: String, new: Any) {}
  open fun onAddLink(type: EventType, id: Long, field: String, link: Long) {}
}