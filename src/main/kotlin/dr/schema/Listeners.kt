package dr.schema

import dr.io.Delete
import dr.io.Insert
import dr.io.Update
import dr.query.QTree

/* ------------------------- enums -------------------------*/
enum class EventType {
  CHECKED, COMMITTED
}

enum class ActionType(val funName: String) {
  CREATE("onCreate"), UPDATE("onUpdate"), DELETE("onDelete"),
  ADD("onAdd"), LINK("onLink"), UNLINK("onUnlink")
}

/* ------------------------- api -------------------------*/
open class EListener {
  open fun onQuery(id: Long, tree: QTree) {}

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