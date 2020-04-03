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

data class QTree(
  val entity: String,

  val filter: QExpression?,
  val limit: Int?,
  val page: Int?,
  val select: QSelect
)

  // filter structure
  data class QExpression(
    val left: QExpression?,
    val oper: OperType?,
    val right: QExpression?,
    val predicate: QPredicate?
  )

    data class QPredicate(
      val path: List<QDeref>,
      val comp: CompType,
      val value: String //TODO: process this
    )

      data class QDeref(
        val entity: String,
        val deref: DerefType,
        val name: String
      )

// select structure
data class QSelect(
  val hasAll: Boolean,
  val fields: List<QField>,
  val relations: List<QRelation>
)

  data class QField(
    val entity: String,
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

