package dr.schema

import dr.DrServer
import dr.action.ActionEngine
import dr.modification.ModificationEngine
import dr.notification.NotificationEngine
import dr.query.QueryEngine
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

enum class ActionType {
  READ, CREATE, UPDATE, DELETE,
  ADD_CREATE, ADD_LINK, REMOVE_LINK
}

/* ------------------------- api -------------------------*/
open class EListener<T>() {
  val qEngine: QueryEngine by lazy { DrServer.qEngine }
  val mEngine: ModificationEngine by lazy { DrServer.mEngine }
  val aEngine: ActionEngine by lazy { DrServer.aEngine }
  val nEngine: NotificationEngine by lazy { DrServer.nEngine }

  open fun onRead(id: Long, tree: Map<String, Any>) {}

  open fun onCreate(type: EventType, id: Long, new: T) {}
  open fun onUpdate(type: EventType, id: Long, tree: Map<String, Any>) {}
  open fun onDelete(type: EventType, id: Long) {}

  open fun onAddCreate(type: EventType, id: Long, field: String, new: Any) {}
  open fun onAddLink(type: EventType, id: Long, field: String, link: Long) {}
  open fun onRemoveLink(type: EventType, id: Long, field: String, link: Long) {}
}