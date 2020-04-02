package dr.query

import dr.schema.*
import dr.spi.IAccessed
import dr.spi.IQueryAdaptor
import dr.spi.IQueryAuthorize
import dr.spi.IQueryExecutor
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.*

class DrQueryEngine(private val schema: Schema, private val adaptor: IQueryAdaptor, private val authorizer: IQueryAuthorize) {
  fun compile(query: String): IQueryExecutor {
    val lexer = QueryLexer(CharStreams.fromString(query))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val tree = parser.query()

    val walker = ParseTreeWalker()
    val listener = DrQueryListener(this.schema, this.authorizer)

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

/* ----------- Helpers ----------- */
private class AccessedPaths(): IAccessed {
  private val paths = mutableMapOf<String, Any>()

  override fun getEntityName() = this.paths.keys.first()

  @Suppress("UNCHECKED_CAST")
  override fun getPaths(): Map<String, Any> {
    val name = getEntityName()
    return this.paths[name] as Map<String, Any>
  }

  fun add(prefix: List<String>, path: TerminalNode): List<String> {
    val full = prefix.plus(path.text)
    addFull(full)
    return full
  }

  fun add(prefix: List<String>, path: List<TerminalNode>): List<String> {
    val full =  path.map{ it.text }.fold(prefix){ pref, value -> pref.plus(value) }
    addFull(full)
    return full
  }

  @Suppress("UNCHECKED_CAST")
  private fun addFull(full: List<String>) {
    var map = this.paths
    for (key in full) {
      map = map.getOrPut(key) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
    }
  }

  override fun toString(): String {
    return "Accessed(entity=${this.getEntityName()}, paths=${this.getPaths()})"
  }
}

private class DrQueryListener(private val schema: Schema, private val authorize: IQueryAuthorize): QueryBaseListener() {
  val errors = mutableListOf<String>()
  var compiled: QTree? = null

  private var accessed: AccessedPaths = AccessedPaths()

  override fun enterQuery(ctx: QueryParser.QueryContext) {
    // check entity name
    val eText = ctx.entity().text
    val entity = this.schema.entities[eText] ?: throw Exception("No entity found: $eText")

    // process query
    val rel = processQLine(listOf(entity.name), ctx.qline(), entity)
    this.compiled = QTree(rel)

    // check query authorization
    this.authorize.authorized(accessed)
  }

  override fun visitErrorNode(error: ErrorNode) {
    this.errors.add(error.text)
  }

  private fun processQLine(prefix: List<String>, qline: QueryParser.QlineContext, sEntity: SEntity): QRelation {
    // process filter paths
    val filter = qline.filter()?.let {
      it.predicate().forEach { p ->
        accessed.add(prefix, p.path().ID())
        // TODO: update for advanced paths! ex: address.{ name == "Paris" } or roles..{ name == "admin" }
      }

      // TODO: process filter predicates
      QFilter(it.text)
    }

    // process select fields
    val (hasAll, fields) = processFields(prefix, qline.select().fields(), sEntity.fields)

    // process select relations
    val relations = processRelations(prefix, qline.select().relation(), sEntity.rels)

    val limit = qline.limit()?.let { it.INT().text.toInt() }
    val page = qline.page()?.let { it.INT().text.toInt() }

    val select = QSelect(hasAll, fields, relations)

    return QRelation(prefix.last(), select, filter, limit, page)
  }

  private fun processRelations(prefix: List<String>, relations: List<QueryParser.RelationContext>, sRelations: Map<String, SRelation>): List<QRelation> {
    val present = mutableSetOf<String>()

    return relations.map {
      val full = accessed.add(prefix, it.ID())
      exists(present, full)

      val rel = sRelations[full.last()] ?: throw Exception("Invalid schema path: ${path(full)}")
      processQLine(full, it.qline(), rel.ref)
    }
  }

  private fun processFields(prefix: List<String>, fields: QueryParser.FieldsContext, sFields: Map<String, SField>): Pair<Boolean, List<QField>> {
    val present = mutableSetOf<String>()

    return if (fields.ALL() != null) {
      val path = accessed.add(prefix, fields.ALL())
      exists(present, path)
      Pair(true, listOf<QField>())
    } else {
      Pair(false, fields.field().map {
        val full = accessed.add(prefix, it.ID())
        exists(present, full)

        if (sFields[full.last()] == null)
          throw Exception("Invalid schema path: ${path(full)}")

        // TODO: set correct sort and order
        QField(it.ID().text, SortType.NONE, 0)
      })
    }
  }

  private fun exists(present: MutableSet<String>, full: List<String>) {
    val path = path(full)
    if (present.contains(path))
      this.errors.add("Path '$path' already exists in select!")
    present.add(path)
  }

  private fun path(full: List<String>): String {
    return full.drop(1).fold(full.first()) { acc, value -> "$acc.$value" }
  }
}