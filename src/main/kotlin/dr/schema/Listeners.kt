package dr.schema

import dr.DrServer
import dr.action.ActionEngine
import dr.modification.Delete
import dr.modification.Insert
import dr.modification.ModificationEngine
import dr.modification.Update
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
  CHECKED, COMMITTED
}

enum class ActionType(val funName: String) {
  CREATE("onCreate"), UPDATE("onUpdate"), DELETE("onDelete"),
  ADD("onAdd"), LINK("onLink"), UNLINK("onUnlink")
}

/* ------------------------- api -------------------------*/
open class EListener<T> {
  val qEngine: QueryEngine by lazy { DrServer.qEngine }
  val mEngine: ModificationEngine by lazy { DrServer.mEngine }
  val aEngine: ActionEngine by lazy { DrServer.aEngine }
  val nEngine: NotificationEngine by lazy { DrServer.nEngine }

  open fun onRead(id: Long, tree: Map<String, Any?>) {}

  open fun onCreate(instruction: Insert) {}
  open fun onUpdate(instruction: Update) {}
  open fun onDelete(instruction: Delete) {}

  open fun onAdd(instruction: Insert) {}

  open fun onLink(instruction: Insert) {}
  open fun onUnlink(instruction: Delete) {}
}

@FunctionalInterface
interface FieldCheck<T> {
  fun check(value: T): String?
}