package dr.query

import dr.schema.*
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.*

class AccessedPaths(val entity: SEntity) {
  private val paths = mutableMapOf<String, Any>()

  fun add(prefix: List<String>, path: TerminalNode): List<String> {
    val full = prefix.plus(path.text)
    addFull(full)
    return full
  }

  fun add(prefix: List<String>, path: List<TerminalNode>): List<String> {
    val full =  path.map{ it.text }.fold(prefix){ prefix, value -> prefix.plus(value) }
    addFull(full)
    return full
  }

  fun check(): List<String> {
    //TODO: return list of errors
    return listOf<String>()
  }

  private fun addFull(full: List<String>) {
    var map = this.paths
    for (key in full) {
      map = map.getOrPut(key) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
    }
  }

  override fun toString(): String {
    return "AccessedPaths(entity=${this.entity.name}, paths=${this.paths})"
  }
}

class DrQueryEngine(private val schema: Schema, private val adaptor: QueryAdaptor) {
  fun compile(query: String): QueryExecutor {
    val lexer = QueryLexer(CharStreams.fromString(query))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val tree = parser.query()

    val walker = ParseTreeWalker()
    val listener = DrQueryListener(this.schema)

    try {
      walker.walk(listener, tree)
    } catch (ex: Exception) {
      throw Exception("Failed to compile query! ${ex.message}")
    }

    if (listener.errors.isNotEmpty()) {
      throw Exception("Failed to compile query! ${listener.errors}")
    }

    return this.adaptor.compile(listener.compiled!!)
  }
}

class DrQueryListener(private val schema: Schema): QueryBaseListener() {
  val errors = mutableListOf<String>()
  var compiled: CompiledQuery? = null

  override fun enterQuery(ctx: QueryParser.QueryContext) {
    // check entity name
    val eText = ctx.entity().text
    val entity = this.schema.entities[eText] ?: throw Exception("No entity found: $eText")

    val accessed = AccessedPaths(entity)
    processQLine(listOf(), ctx.qline(), accessed)

    println(accessed)
    // TODO: check if paths exist?

    // TODO: check access authorization from paths?

    this.compiled = CompiledQuery(ctx)
  }

  override fun visitErrorNode(error: ErrorNode) {
    this.errors.add(error.text)
  }

  private fun processQLine(prefix: List<String>, qline: QueryParser.QlineContext, accessed: AccessedPaths) {
    // process filter paths
    val present = mutableSetOf<String>()
    qline.filter()?.let {
      it.predicate().forEach { p ->
        val path = accessed.add(prefix, p.path().ID())
        exists(present, path, "filter")
        // TODO: update for advanced paths! ex: address.{ name == "Paris" } or roles..{ name == "admin" }
      }
    }

    // process select fields
    val fields = qline.select().fields()
    processFields(prefix, fields, accessed)

    // process select relations
    val relations = qline.select().relation()
    processRelations(prefix, relations, accessed)
  }

  private fun processRelations(prefix: List<String>, relations: List<QueryParser.RelationContext>, accessed: AccessedPaths) {
    val present = mutableSetOf<String>()

    relations.forEach {
      val path = accessed.add(prefix, it.ID())
      exists(present, path, "select")
      processQLine(path, it.qline(), accessed)
    }
  }

  private fun processFields(prefix: List<String>, fields: QueryParser.FieldsContext, accessed: AccessedPaths) {
    val present = mutableSetOf<String>()

    if (fields.ALL() != null) {
      val path = accessed.add(prefix, fields.ALL())
      exists(present, path, "select")
    } else {
      fields.field().forEach {
        val path = accessed.add(prefix, it.ID())
        exists(present, path, "select")
      }
    }
  }

  private fun exists(present: MutableSet<String>, full: List<String>, pos: String) {
    val path = full.drop(1).fold(full.first()) { acc, value -> "$acc.$value" }
    if (present.contains(path))
      this.errors.add("Path '$path' already exists in $pos!")
    present.add(path)
  }
}