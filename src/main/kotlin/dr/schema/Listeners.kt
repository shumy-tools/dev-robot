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

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Checks(vararg val value: KClass<out Any>)

/* ------------------------- enums -------------------------*/
enum class EventType {
  CHECKED, COMMITED
}

enum class ActionType(val funName: String) {
  CREATE("onCreate"), UPDATE("onUpdate"), DELETE("onDelete"),
  ADD("onAdd"), LINK("onLink"), REMOVE("onRemove")
}

/* ------------------------- api -------------------------*/
open class EListener<T> {
  val qEngine: QueryEngine by lazy { DrServer.qEngine }
  val mEngine: ModificationEngine by lazy { DrServer.mEngine }
  val aEngine: ActionEngine by lazy { DrServer.aEngine }
  val nEngine: NotificationEngine by lazy { DrServer.nEngine }

  open fun onRead(id: Long, tree: Map<String, Any>) {}

  open fun onCreate(type: EventType, id: Long?, new: T) {}
  open fun onUpdate(type: EventType, id: Long, data: Map<String, Any?>) {}
  open fun onDelete(type: EventType, id: Long) {}

  open fun onAdd(type: EventType, id: Long?, sRelation: SRelation, link: Long?, new: Any) {}
  open fun onLink(type: EventType, id: Long?, sRelation: SRelation, new: Any) {}
  open fun onRemove(type: EventType, id: Long, sRelation: SRelation, link: Long) {}
}

@FunctionalInterface
interface FieldCheck<T> {
  fun check(value: T): String?
}