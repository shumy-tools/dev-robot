package dr.spi

import dr.schema.SEntity
import dr.schema.SRelation

interface IModificationAdaptor {
  fun start(): ITransaction
}

  interface ITransaction {
    fun create(sEntity: SEntity, new: Any): Long
    fun update(sEntity: SEntity, id: Long, data: Map<String, Any?>)

    fun add(sEntity: SEntity, id: Long, sRelation: SRelation, new: Any): Long
    fun link(sEntity: SEntity, id: Long, sRelation: SRelation, link: Long)
    fun remove(sEntity: SEntity, id: Long, sRelation: SRelation, link: Long)

    fun commit()
    fun rollback()
  }