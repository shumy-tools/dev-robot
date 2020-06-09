package dr.query

import dr.schema.PARENT
import dr.schema.SUPER
import dr.schema.tabular.STable

enum class SortType {
  NONE, ASC, DSC
}

enum class OperType {
  OR, AND
}

enum class CompType {
  EQUAL, DIFFERENT, MORE, LESS, MORE_EQ, LESS_EQ, IN
}

enum class DerefType {
  FIELD, ONE, MANY
}

// This enum must contain all options from dr.schema.FieldType
enum class ParamType {
  TEXT, INT, FLOAT, BOOL,
  TIME, DATE, DATETIME,
  LIST, PARAM, LP_PARAM
}

// ----------- query structure -----------
data class QTree(
  val table: STable,

  val filter: QExpression?,
  val limit: QParam?,
  val page: QParam?,
  val select: QSelect
) {
  constructor(table: STable, rel: QRelation): this(table, rel.filter, rel.limit, rel.page, rel.select)
}

  // ----------- filter structure -----------
  data class QExpression(
    val left: QExpression?,
    val oper: OperType?,
    val right: QExpression?,
    val predicate: QPredicate?
  )

    data class QPredicate(
      val path: List<QDeref>,
      val comp: CompType,
      val param: QParam
    )

      data class QDeref(
        val table: STable,
        val deref: DerefType,
        val name: String
      )

      data class QParam(
        val type: ParamType,
        val value: Any
      )

  // ----------- select structure -----------
  data class QSelect(
    val hasAll: Boolean,
    val fields: List<QField>,
    val relations: List<QRelation>
  ) {
    val superRef = relations.find { it.name == SUPER }
    val parentRef = relations.find { it.name == PARENT }
  }

    data class QField(
      val name: String,
      val jType: Class<out Any>,

      val sort: SortType,
      val order: Int
    )

    data class QRelation(
      val name: String,
      val ref: STable,

      val filter: QExpression?,
      val limit: QParam?,
      val page: QParam?,
      val select: QSelect
    )

