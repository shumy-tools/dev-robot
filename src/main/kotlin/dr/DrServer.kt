package dr

import dr.action.ActionEngine
import dr.io.DEntityTranslator
import dr.io.InputProcessor
import dr.modification.ModificationEngine
import dr.notification.NotificationEngine
import dr.query.QueryEngine
import dr.schema.Schema

object DrServer {
  var enabled = false
    private set

  lateinit var schema: Schema
  lateinit var processor: InputProcessor
  lateinit var translator: DEntityTranslator

  lateinit var qEngine: QueryEngine
  lateinit var mEngine: ModificationEngine
  lateinit var aEngine: ActionEngine
  lateinit var nEngine: NotificationEngine

  fun start(port: Int) {
    processor = InputProcessor(schema)
    translator = DEntityTranslator(schema)

    enabled = true
  }
}