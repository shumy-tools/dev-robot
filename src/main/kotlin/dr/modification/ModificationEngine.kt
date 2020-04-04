package dr.modification

import dr.DrServer
import dr.schema.Schema

/* ------------------------- api -------------------------*/
class ModificationEngine() {
  private val schema: Schema by lazy { DrServer.schema }
}