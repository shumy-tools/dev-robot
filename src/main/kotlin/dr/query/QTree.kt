package dr.query

import dr.schema.SEntity

enum class SortType {
  NONE, ASC, DSC
}

data class QTree(val relation: QRelation)

data class QFilter(
  val filter: String
)

data class QSelect(
  val hasAll: Boolean,
  val fields: List<QField>,
  val relations: List<QRelation>
)

data class QField(
  val name: String,
  val sort: SortType,
  val order: Int
)

data class QRelation(
  val name: String,
  val select: QSelect,

  val filter: QFilter?,
  val limit: Int?,
  val page: Int?
)

