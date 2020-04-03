package dr.query

import dr.schema.*
import dr.spi.IAccessed
import dr.spi.IQueryAdaptor
import dr.spi.IQueryAuthorize
import dr.spi.IQueryExecutor
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.*
import java.lang.StringBuilder

class QEngine(private val schema: Schema, private val adaptor: IQueryAdaptor, private val authorizer: IQueryAuthorize) {
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

  fun addField(prefix: List<String>, path: String): List<String> {
    val full = prefix.plus(path)
    addFull(full, true)
    return full
  }

  fun addRelation(prefix: List<String>, path: String): List<String> {
    val full = prefix.plus(path)
    addFull(full, false)
    return full
  }

  @Suppress("UNCHECKED_CAST")
  private fun addFull(full: List<String>, withIgnore: Boolean) {
    var map = this.paths
    var last = map
    for (key in full) {
      last = map
      map = map.getOrPut(key) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
    }

    if (withIgnore && full.last() != "*" && last.containsKey("*"))
      last.remove(full.last())
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
    // process select fields
    val (hasAll, fields) = processFields(prefix, qline.select().fields(), sEntity)

    // process select relations
    val relations = processRelations(prefix, qline.select().relation(), sEntity)

    val limit = qline.limit()?.let {
      val limit = it.INT().text.toInt()
      if (limit < 0)
        throw Exception("Limit must be > 0")
      limit
    }

    val page = qline.page()?.let {
      val page = it.INT().text.toInt()
      if (page < 0)
        throw Exception("Page must be > 0")
      page
    }

    // process filter paths
    val filter = qline.filter()?.let { processFilter(prefix, it, sEntity) }

    val select = QSelect(hasAll, fields, relations)
    return QRelation(prefix.last(), select, filter, limit, page)
  }

  private fun processRelations(prefix: List<String>, relations: List<QueryParser.RelationContext>, sEntity: SEntity): List<QRelation> {
    val present = mutableSetOf<String>()

    return relations.map {
      val path = it.ID().text
      val full = accessed.addRelation(prefix, path)
      exists(present, full)

      val rel = sEntity.rels[path] ?: throw Exception("Invalid relation path '${sEntity.name}.$path'")
      processQLine(full, it.qline(), rel.ref)
    }
  }

  private fun processFields(prefix: List<String>, fields: QueryParser.FieldsContext, sEntity: SEntity): Pair<Boolean, List<QField>> {
    val present = mutableSetOf<String>()

    return if (fields.ALL() != null) {
      val path = accessed.addField(prefix, fields.ALL().text)
      exists(present, path)
      Pair(true, listOf<QField>())
    } else {
      Pair(false, fields.field().map {
        val path = it.ID().text
        val full = accessed.addField(prefix, path)
        exists(present, full)

        if (sEntity.fields[path] == null)
          throw Exception("Invalid field path '${sEntity.name}.$path'")

        val sort = when(it.order()?.text) {
          "asc" -> SortType.ASC
          "dsc" -> SortType.DSC
          else -> SortType.NONE
        }

        val order = it.INT()?.let { ord ->
          val order = ord.text.toInt()
          if (order < 0)
            throw Exception("Order must be > 0")
          order
        } ?: 0

        QField(it.ID().text, sort, order)
      })
    }
  }

  private fun processFilter(prefix: List<String>, filter: QueryParser.FilterContext, sEntity: SEntity): QFilter {
    println("predicate: ${filter.predicate().text}")
    println("more: ${filter.more().size}")

    filter.more().forEach {
      val logic = it.logic().text
      val predicate = it.predicate()

      if (predicate.path().next().isEmpty()) {
        val path = predicate.path().ID().text
        accessed.addField(prefix, path)
        val sField = sEntity.fields[path] ?: throw Exception("Invalid field path '${sEntity.name}.$path'")
        // TODO: check sField operator constraints
      } else {
        val last = predicate.path().next().last()
        val (lEntity, lRelation, full) = processPath(prefix, predicate.path(), sEntity)

        val deref = last.deref().text
        checkDeref(deref, lEntity, full.last(), lRelation)

        val path = last.ID().text
        accessed.addField(prefix, path)
        val sField = lEntity.fields[path] ?: throw Exception("Invalid field path '${lEntity.name}.$path'")
        // TODO: check sField operator constraints

      }

      // TODO: update for advanced paths! ex: address.{ name == "Paris" } or roles..{ name == "admin" }
    }

    // TODO: include filter predicates
    return QFilter(filter.text)
  }

  fun processPredicate() {

  }

  private fun processPath(prefix: List<String>, qPath: QueryParser.PathContext, sEntity: SEntity): Triple<SEntity, SRelation, List<String>> {
    var path = qPath.ID().text
    var full = accessed.addField(prefix, path)

    var lEntity = sEntity
    var lRelation = lEntity.rels[path] ?: throw Exception("Invalid relation path '${lEntity.name}.$path'")

    for (nxt in qPath.next().dropLast(1)) {
      val deref = nxt.deref().text
      checkDeref(deref, lEntity, path, lRelation)

      // process next path
      path = nxt.ID().text
      full = accessed.addRelation(full, path)

      lEntity = lRelation.ref
      lRelation = lEntity.rels[path] ?: throw Exception("Invalid relation path '${lEntity.name}.$path'")
    }

    return Triple(lEntity, lRelation, full)
  }

  private fun checkDeref(deref: String, sEntity: SEntity, path: String, sRelation: SRelation) {
    println("checkDeref")
    if (deref == "." && sRelation.isCollection)
      throw Exception("Invalid relation path '${sEntity.name}.${path}..'. Expected one-to-one relation!")

    if (deref == ".." && !sRelation.isCollection)
      throw Exception("Invalid relation path '${sEntity.name}.${path}.' Expected one-to-many relation!")
  }

  private fun exists(present: MutableSet<String>, full: List<String>) {
    val path = full.drop(1).fold(full.first()) { acc, value -> "$acc.$value" }
    if (present.contains(path))
      this.errors.add("Path '$path' already exists in select!")
    present.add(path)
  }
}