package dr

import dr.action.ActionEngine
import dr.modification.ModificationEngine
import dr.notification.NotificationEngine
import dr.query.QueryEngine
import dr.schema.Schema

class DrServer(val schema: Schema) {
  private val tableTranslator = schema.entities.map { (name, _) ->
    name to name.replace('.', '_').toLowerCase()
  }.toMap()

  var enabled = false
    private set

  lateinit var qEngine: QueryEngine
  lateinit var mEngine: ModificationEngine
  lateinit var aEngine: ActionEngine
  lateinit var nEngine: NotificationEngine

  fun start(port: Int) {
    qEngine.schema = schema
    mEngine.schema = schema
    mEngine.tableTranslator = tableTranslator

    this.enabled = true
  }
}