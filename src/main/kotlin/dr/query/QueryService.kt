package dr.query

import dr.schema.TRAITS
import dr.schema.tabular.STable
import dr.schema.tabular.Tables
import dr.spi.IAdaptor
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeWalker
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/* ------------------------- api -------------------------*/
class QueryService(private val tables: Tables, private val adaptor: IAdaptor) {
  fun compile(query: String): Pair<IQueryExecutor<Any>, IReadAccess> {
    val lexer = QueryLexer(CharStreams.fromString(query))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val tree = parser.query()

    val walker = ParseTreeWalker()
    val listener = DrQueryListener(tables)

    walker.walk(listener, tree)
    if (listener.errors.isNotEmpty())
      throw Exception("Failed to compile query! ${listener.errors}")

    val native = this.adaptor.compile(listener.compiled!!)
    return Pair(QueryExecutorWithValidator(native, listener.parameters), listener.accessed)
  }
}

/* ------------------------- helpers -------------------------*/
private class AccessedPaths: IReadAccess {
  override lateinit var entity: String
  override val paths = mutableMapOf<String, Any>()

  @Suppress("UNCHECKED_CAST")
  fun addPath(prefix: List<String>, path: String): List<String> {
    var map = paths
    val full = prefix.plus(path)
    for (key in full) {
      if (map.containsKey("*")) break
      map = map.getOrPut(key) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
    }

    return full
  }

  override fun toString(): String {
    return "Accessed(entity=$entity, paths=$paths)"
  }
}

private class DrQueryListener(private val tables: Tables): QueryBaseListener() {
  val errors = mutableListOf<String>()
  val parameters = mutableListOf<Parameter>()

  var compiled: QTree? = null

  val accessed: AccessedPaths = AccessedPaths()

  override fun enterQuery(ctx: QueryParser.QueryContext) {
    val eText = ctx.entity().text
    val sTable = tables.get(eText)

    accessed.entity = eText
    val rel = sTable.processQLine(listOf(), ctx.qline())
    this.compiled = QTree(sTable, rel)
  }

  override fun visitErrorNode(error: ErrorNode) {
    this.errors.add(error.text)
  }

  private fun STable.processQLine(prefix: List<String>, qline: QueryParser.QlineContext, isDirectRef: Boolean = false): QRelation {
    // process select fields
    val (hasAll, fields) = processFields(prefix, qline.select().fields())

    // process select relations
    val relations = processRelations(prefix, qline.select().relation())

    val limit = qline.limit()?.let {
      if (isDirectRef)
        throw Exception("Direct references don't support 'limit'! Use it on the top entity.")
      processLimitOrPage(it.intOrParam())
    }

    val page = qline.page()?.let {
      if (isDirectRef)
        throw Exception("Direct references don't support 'page'! Use it on the top entity.")
      processLimitOrPage(it.intOrParam())
    }

    // process filter paths
    val filter = qline.filter()?.let { processExpr(prefix, it.expr()) }
    if (isDirectRef && filter != null)
      throw Exception("Direct references don't support 'filter'! Use it on the top entity.")

    val qName = if (prefix.isEmpty()) accessed.entity else prefix.last()
    val qSelect = QSelect(hasAll, fields, relations)
    return QRelation(qName, this, filter, limit, page, qSelect)
  }

  private fun STable.processLimitOrPage(lpCtx: QueryParser.IntOrParamContext): QParam {
    return if (lpCtx.INT() != null) {
      val value = lpCtx.INT().text.toInt()
      if (value < 1)
        throw Exception("'limit' and 'page' must be > 0")
      QParam(ParamType.INT, value)
    } else {
      val value = lpCtx.PARAM().text.substring(1)
      val qParam = QParam(ParamType.LP_PARAM, value)
      parameters.add(Parameter(this, "@lp", CompType.EQUAL, qParam))
      qParam
    }
  }

  private fun STable.processRelations(prefix: List<String>, relations: List<QueryParser.RelationContext>): List<QRelation> {
    val present = mutableSetOf<String>()

    return relations.map {
      val path = it.ID().text
      val full = accessed.addPath(prefix, path)
      exists(present, full)

      val rel = sEntity.rels[path] ?: throw Exception("Invalid relation path '$name.$path'")
      tables.get(rel.ref).processQLine(full, it.qline(), !rel.isCollection)
    }
  }

  private fun STable.processFields(prefix: List<String>, fields: QueryParser.FieldsContext): Pair<Boolean, List<QField>> {
    val present = mutableSetOf<String>()

    return if (fields.ALL() != null) {
      val path = accessed.addPath(prefix, fields.ALL().text)
      exists(present, path)
      Pair(true, listOf())
    } else {
      Pair(false, fields.field().map {
        val path = it.name().text
        val full = accessed.addPath(prefix, path)
        exists(present, full)

        val jType = sEntity.fields[path]?.jType ?: if (path.startsWith(TRAITS)) {
          // TODO: check if trait exists as a field?

          val trait = path.substring(1)
          if (tables.schema.traits[trait] == null)
            throw Exception("Trait not found '$name.$path'")

          String::class.java
        } else throw Exception("Invalid field path '$name.$path'")

        val sortType = sortType(it.order()?.text)
        val order = it.INT()?.let { ord ->
          val order = ord.text.toInt()
          if (order < 1)
            throw Exception("Order must be > 0")
          order
        } ?: 0

        QField(path, jType, sortType, order)
      })
    }
  }

  private fun STable.processExpr(prefix: List<String>, expr: QueryParser.ExprContext): QExpression {
    val list = expr.expr()
    val oper = expr.oper

    return when {
      list.size == 1 -> {
        val qExpression = processExpr(prefix, list.last())
        QExpression(qExpression, null, null, null)
      }

      oper != null -> {
        val qLeft = processExpr(prefix, expr.left)
        val qRight = processExpr(prefix, expr.right)
        val operType = operType(oper.text)
        QExpression(qLeft, operType, qRight, null)
      }

      else -> {
        val qPredicate = processPredicate(prefix, expr.predicate())
        QExpression(null, null, null, qPredicate)
      }
    }
  }

  private fun STable.processPredicate(prefix: List<String>, predicate: QueryParser.PredicateContext): QPredicate {
    var lTable = this
    var nextFull = prefix

    val qDerefList = mutableListOf<QDeref>()
    predicate.path().ID().forEach {
      val nextPath = it.text
      nextFull = accessed.addPath(nextFull, nextPath)

      qDerefList.lastOrNull()?.let { last ->
        // After a field there are no relations!
        if (last.deref == DerefType.FIELD)
          throw Exception("Invalid path '${lTable.name}.${last.name}.$nextPath'")
      }

      val (drType, lRelation) = if (lTable.sEntity.fields.containsKey(nextPath)) Pair(DerefType.FIELD, null) else {
        val sRelation = lTable.sEntity.rels[nextPath] ?: throw Exception("Invalid path '${lTable.name}.$nextPath'")
        if (sRelation.isCollection) Pair(DerefType.MANY, sRelation) else Pair(DerefType.ONE, sRelation)
      }

      qDerefList.add(QDeref(lTable, drType, nextPath))
      if (lRelation != null) lTable = tables.get(lRelation.ref)
    }

    // TODO: support for advanced path-filters? ex: address.| country == "Portugal" and city == "Aveiro" |

    // process comparator and parameter
    val qDerefField = qDerefList.last()
    if (qDerefField.deref != DerefType.FIELD)
      throw Exception("Invalid field '${qDerefField.table.name}.${qDerefField.name}'. A path must always end in a field!")

    val compType = compType(predicate.comp().text)
    val qParam = transformParam(predicate.param())

    if (qDerefField.table.sEntity.fields[qDerefField.name] == null) {
      val rel = qDerefField.table.sEntity.rels[qDerefField.name]
      val completeError = if (rel != null) {
        val oneOf = rel.ref.fields.map { it.key }.plus(rel.ref.rels.map { it.key })
        " However, a relation with that name exists. Please complete the path with one of $oneOf"
      } else ""

      throw Exception("Invalid relation path '${qDerefField.table.name}.${qDerefField.name}'!$completeError")
    }

    parameters.add(Parameter(qDerefField.table, qDerefField.name, compType, qParam))

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

private fun transformParam(param: QueryParser.ParamContext) = when {
  param.value() != null -> transformValue(param.value())
  param.list() != null -> QParam(ParamType.LIST, param.list().value().map { transformValue(it).value })
  else -> throw Exception("Unrecognized parameter type!")
}

private fun transformValue(value: QueryParser.ValueContext) = when {
  value.TEXT() != null -> QParam(ParamType.TEXT, value.TEXT().text.substring(1, value.TEXT().text.length-1))
  value.INT() != null -> QParam(ParamType.INT, value.INT().text.toInt())
  value.FLOAT() != null -> QParam(ParamType.FLOAT, value.FLOAT().text.toFloat())
  value.BOOL() != null -> QParam(ParamType.BOOL, value.BOOL().text!!.toBoolean())
  value.TIME() != null -> QParam(ParamType.TIME, LocalTime.parse(value.TIME().text.substring(1)))
  value.DATE() != null -> QParam(ParamType.DATE, LocalDate.parse(value.DATE().text.substring(1)))
  value.DATETIME() != null -> QParam(ParamType.DATETIME, LocalDateTime.parse(value.DATETIME().text.substring(1)))
  value.PARAM() != null -> QParam(ParamType.PARAM, value.PARAM().text.substring(1))
  else -> throw Exception("Unrecognized parameter type!")
}