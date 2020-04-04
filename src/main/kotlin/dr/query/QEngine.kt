package dr.query

import dr.schema.*
import dr.spi.*
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/* ------------------------- api -------------------------*/
class QEngine(private val schema: Schema, private val adaptor: IQueryAdaptor, private val authorizer: IQueryAuthorizer) {
  fun compile(query: String): IQueryExecutor {
    val lexer = QueryLexer(CharStreams.fromString(query))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val tree = parser.query()

    val walker = ParseTreeWalker()
    val listener = DrQueryListener(this.schema, this.authorizer)

    walker.walk(listener, tree)
    //println("tokens: ${tokens.tokens.map { it.text }}")

    if (listener.errors.isNotEmpty())
      throw Exception("Failed to compile query! ${listener.errors}")

    val native = this.adaptor.compile(listener.compiled!!)
    return QueryExecutorWithValidator(native, listener.parameters)
  }
}

/* ------------------------- helpers -------------------------*/
private class AccessedPaths : IAccessed {
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

private class DrQueryListener(private val schema: Schema, private val authorize: IQueryAuthorizer): QueryBaseListener() {
  val errors = mutableListOf<String>()
  val parameters = mutableListOf<Parameter>()

  var compiled: QTree? = null

  private var accessed: AccessedPaths = AccessedPaths()

  override fun enterQuery(ctx: QueryParser.QueryContext) {
    // check entity name
    val eText = ctx.entity().text
    val entity = this.schema.entities[eText] ?: throw Exception("No entity found: $eText")

    // process query
    val rel = processQLine(listOf(eText), ctx.qline(), entity, entity)
    this.compiled = QTree(eText, rel.filter, rel.limit, rel.page, rel.select)

    // check query authorization
    this.authorize.authorize(accessed)
  }

  override fun visitErrorNode(error: ErrorNode) {
    this.errors.add(error.text)
  }

  private fun processQLine(prefix: List<String>, qline: QueryParser.QlineContext, lEntity: SEntity, sEntity: SEntity): QRelation {
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
    val filter = qline.filter()?.let { processExpr(prefix, it.expr(), sEntity) }

    val select = QSelect(hasAll, fields, relations)
    return QRelation(lEntity.name, prefix.last(), filter, limit, page, select)
  }

  private fun processRelations(prefix: List<String>, relations: List<QueryParser.RelationContext>, sEntity: SEntity): List<QRelation> {
    val present = mutableSetOf<String>()

    return relations.map {
      val path = it.ID().text
      val full = accessed.addRelation(prefix, path)
      exists(present, full)

      val rel = sEntity.rels[path] ?: throw Exception("Invalid relation path '${sEntity.name}.$path'")
      processQLine(full, it.qline(), sEntity, rel.ref)
    }
  }

  private fun processFields(prefix: List<String>, fields: QueryParser.FieldsContext, sEntity: SEntity): Pair<Boolean, List<QField>> {
    val present = mutableSetOf<String>()

    return if (fields.ALL() != null) {
      val path = accessed.addField(prefix, fields.ALL().text)
      exists(present, path)
      Pair(true, listOf())
    } else {
      Pair(false, fields.field().map {
        val path = it.ID().text
        val full = accessed.addField(prefix, path)
        exists(present, full)

        if (sEntity.fields[path] == null)
          throw Exception("Invalid field path '${sEntity.name}.$path'")

        val sortType = sortType(it.order()?.text)

        val order = it.INT()?.let { ord ->
          val order = ord.text.toInt()
          if (order < 0)
            throw Exception("Order must be > 0")
          order
        } ?: 0

        QField(sEntity.name, path, sortType, order)
      })
    }
  }

  private fun processExpr(prefix: List<String>, expr: QueryParser.ExprContext, sEntity: SEntity): QExpression {
    val list = expr.expr()
    val oper = expr.oper

    return when {
      list.size == 1 -> {
        val qExpression = processExpr(prefix, list.last(), sEntity)
        QExpression(qExpression, null, null, null)
      }

      oper != null -> {
        val qLeft = processExpr(prefix, expr.left, sEntity)
        val qRight = processExpr(prefix, expr.right, sEntity)
        val operType = operType(oper.text)

        QExpression(qLeft, operType, qRight, null)
      }

      else -> {
        val qPredicate = processPredicate(prefix, expr.predicate(), sEntity)
        QExpression(null, null, null, qPredicate)
      }
    }
  }

  private fun processPredicate(prefix: List<String>, predicate: QueryParser.PredicateContext, sEntity: SEntity): QPredicate {
    // process head
    val path = predicate.path().ID().text
    val full = accessed.addField(prefix, path)
    val qDerefHead = QDeref(sEntity.name, DerefType.ONE, path)

    // process tail
    val fullPath = predicate.path().next()
    val qDerefList = if (fullPath.isNotEmpty()) {
      var nextPath = path
      var nextFull = full
      var lEntity = sEntity
      var lRelation = lEntity.rels[path] ?: throw Exception("Invalid relation path '${lEntity.name}.$path'")

      val qDerefTail = fullPath.mapIndexed { index, nxt ->
        val drType = derefType(nxt.deref().text)
        checkDeref(drType, lEntity, nextPath, lRelation)

        // process next path
        nextPath = nxt.ID().text
        nextFull = accessed.addRelation(nextFull, nextPath)

        lEntity = lRelation.ref
        if (index != fullPath.size - 1) {
          lRelation = lEntity.rels[nextPath] ?: throw Exception("Invalid relation path '${lEntity.name}.$nextPath'")
        }

        QDeref(lEntity.name, drType, nextPath)
      }

      listOf(qDerefHead).plus(qDerefTail)
    } else {
      listOf(qDerefHead)
    }

    // TODO: update qDerefList with advanced paths? ex: address.{ name == "Paris" } or roles..{ name == "admin" }

    // process comparator and parameter
    val qDeref = qDerefList.last()
    val compType = compType(predicate.comp().text)
    val qParam = transformParam(predicate.param())

    val lEntity = schema.entities[qDeref.entity]
    this.parameters.add(Parameter(lEntity!!, qDeref.name, compType, qParam))

    return QPredicate(qDerefList, compType, qParam)
  }

  private fun exists(present: MutableSet<String>, full: List<String>) {
    val path = full.drop(1).fold(full.first()) { acc, value -> "$acc.$value" }
    if (present.contains(path))
      this.errors.add("Path '$path' already exists in select!")
    present.add(path)
  }
}


/* ------------------------- helpers -------------------------*/
private fun sortType(sort: String?) = when(sort) {
  "asc" -> SortType.ASC
  "dsc" -> SortType.DSC
  null -> SortType.NONE
  else -> throw Exception("Unrecognized sort operator! Use (asc, dsc)")
}


private fun operType(oper: String) = when(oper) {
  "or" -> OperType.OR
  "and" -> OperType.AND
  else -> throw Exception("Unrecognized logic operator! Use (or, and)")
}

private fun compType(comp: String) = when (comp) {
  "==" -> CompType.EQUAL
  "!=" -> CompType.DIFFERENT
  ">" -> CompType.MORE
  "<" -> CompType.LESS
  ">=" -> CompType.MORE_EQ
  "<=" -> CompType.LESS_EQ
  "in" -> CompType.IN
  else -> throw Exception("Unrecognized comparator! Use ('==', '!=', '>', '<', '>=', '<=', 'in')")
}

private fun derefType(deref: String) = when (deref) {
  "." -> DerefType.ONE
  ".." -> DerefType.MANY
  else -> throw Exception("Unrecognized deref operator! Use ('.', '..')")
}

private fun transformParam(param: QueryParser.ParamContext) = when {
  param.value() != null -> transformValue(param.value())
  param.list() != null -> QParam(ParamType.LIST, param.list().value().map { transformValue(it).value })
  else -> throw Exception("Unrecognized parameter type!")
}

private fun transformValue(value: QueryParser.ValueContext) = when {
  value.TEXT() != null -> QParam(ParamType.TEXT, value.TEXT().text)
  value.INT() != null -> QParam(ParamType.INT, value.INT().text.toLong())
  value.FLOAT() != null -> QParam(ParamType.FLOAT, value.FLOAT().text.toDouble())
  value.BOOL() != null -> QParam(ParamType.BOOL, value.BOOL().text.toBoolean())
  value.TIME() != null -> QParam(ParamType.TIME, LocalTime.parse(value.TIME().text.substring(1)))
  value.DATE() != null -> QParam(ParamType.DATE, LocalDate.parse(value.DATE().text.substring(1)))
  value.DATETIME() != null -> QParam(ParamType.DATETIME, LocalDateTime.parse(value.DATETIME().text.substring(1)))
  value.PARAM() != null -> QParam(ParamType.PARAM, value.PARAM().text.substring(1))
  else -> throw Exception("Unrecognized parameter type!")
}

private fun checkDeref(drType: DerefType, sEntity: SEntity, path: String, sRelation: SRelation) {
  if (drType == DerefType.ONE && sRelation.isCollection)
    throw Exception("Invalid relation path '${sEntity.name}.${path}.'. Expected one-to-many relation!")

  if (drType == DerefType.MANY && !sRelation.isCollection)
    throw Exception("Invalid relation path '${sEntity.name}.${path}..' Expected one-to-one relation!")
}