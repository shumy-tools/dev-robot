package dr.query

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
  ONE, MANY
}

// This enum must contain all options from dr.schema.FieldType
enum class ParamType {
  TEXT, INT, FLOAT, BOOL,
  TIME, DATE, DATETIME,
  LIST, PARAM
}

// ----------- query structure -----------
data class QTree(
  val entity: String,

  val filter: QExpression?,
  val limit: Int?,
  val page: Int?,
  val select: QSelect
)

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
        val entity: String,
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
  )

    data class QField(
      val table: String,
      val name: String,

      val sort: SortType,
      val order: Int
    )

    data class QRelation(
      val entity: String,
      val name: String,

      val filter: QExpression?,
      val limit: Int?,
      val page: Int?,
      val select: QSelect
    )

