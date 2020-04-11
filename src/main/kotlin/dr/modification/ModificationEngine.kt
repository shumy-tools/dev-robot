package dr.modification

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.annotation.JsonUnwrapped
import dr.DrServer
import dr.schema.*
import dr.spi.IModificationAdaptor

/* ------------------------- api -------------------------*/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class LinkData
  sealed class LinkCreate: LinkData()
    @JsonTypeName("many-link")
    class ManyLinksWithoutTraits(val refs: Collection<Long>): LinkCreate() {
      constructor(vararg values: Long): this(values.toList())
    }

    @JsonTypeName("one-link")
    class OneLinkWithoutTraits(val ref: Long): LinkCreate()

    @JsonTypeName("many-link-traits")
    class ManyLinksWithTraits(val refs: Map<Long, Pack>): LinkCreate() {
      constructor(vararg values: Pair<Long, Pack>): this(values.toMap())
    }

    // hack? jackson doesn't support Creator with @JsonUnwrapped
    @JsonTypeName("one-link-traits")
    class OneLinkWithTraits(val ref: Long): LinkCreate() {
      @JsonUnwrapped
      lateinit var traits: Pack
        private set

      constructor(ref: Long, traits: Pack): this(ref) {
        this.traits = traits
      }
    }

  sealed class LinkDelete: LinkData()
    @JsonTypeName("many-unlink")
    class ManyLinkDelete(val links: Collection<Long>): LinkDelete() {
      constructor(vararg values: Long): this(values.toList())
    }

    @JsonTypeName("one-unlink")
    class OneLinkDelete(val link: Long): LinkDelete()

class UpdateData(
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
  val data: Map<String, Any?>
) {
  constructor(vararg pairs: Pair<String, Any?>): this(pairs.toMap())
}


class ModificationEngine(private val adaptor: IModificationAdaptor) {
  private val schema by lazy { DrServer.schema }
  private val tables by lazy { DrServer.tableTranslator }

  fun create(new: Any): Long {
    // create master entity
    val instructions = InstructionBuilder(schema, tables).apply {
      val insert = createEntity(new)
      addRoot(insert)
    }.build()

    // commit instructions
    instructions.fireCheckedListeners()
    val id = adaptor.commit(instructions).first()
    instructions.fireCommittedListeners()

    return id
  }

  fun update(entity: String, id: Long, new: UpdateData) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")

    // update entity
    val instructions = InstructionBuilder(schema, tables).apply {
      val update = updateEntity(sEntity, id, new.data)
      addRoot(update)
    }.build()

    // commit instructions
    instructions.fireCheckedListeners()
    adaptor.commit(instructions)
    instructions.fireCommittedListeners()
  }

  fun add(entity: String, id: Long, rel: String, new: Any): List<Long> {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.CREATE)
      throw Exception("Relation is not 'create'! - ($entity, $rel)")

    // creates one/many related entities
    val instructions = InstructionBuilder(schema, tables).apply {
      val roots = addRelations(sEntity, sRelation, new, resolvedInv = id)
      addRoots(roots)
    }.build()

    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
  }

  fun link(entity: String, id: Long, rel: String, data: LinkCreate): List<Long> {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    // creates one/many links
    val instructions = InstructionBuilder(schema, tables).apply {
      val roots = addLinks(sEntity, sRelation, data, resolvedInv = id)
      addRoots(roots)
    }.build()


    // commit instructions
    instructions.fireCheckedListeners()
    val ids = adaptor.commit(instructions)
    instructions.fireCommittedListeners()

    return ids
  }

  fun unlink(entity: String, rel: String, data: LinkDelete) {
    val (sEntity, sRelation) = checkRelationChange(entity, rel)
    if (sRelation.type != RelationType.LINK)
      throw Exception("Relation is not 'link'! - ($entity, $rel)")

    // delete one/many links
    val instructions = InstructionBuilder(schema, tables).apply {
      removeLinks(sEntity, sRelation, data)
    }.build()

    // commit instructions
    instructions.fireCheckedListeners()
    adaptor.commit(instructions)
    instructions.fireCommittedListeners()
  }

  fun check(entity: String, field: String, value: Any?) {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sField = sEntity.fields[field] ?: throw Exception("Entity field not found! - ($entity, $field)")

    if (!sField.isInput)
      throw Exception("Invalid input field! - (${sEntity.name}, $field)")

    if (!sField.isOptional && value == null)
      throw Exception("Invalid 'null' input! - (${sEntity.name}, $field)")

    value?.let {
      val type =  it.javaClass.kotlin
      if (!TypeEngine.check(sField.type, type))
        throw Exception("Invalid field type, expected ${sField.type} found ${type.simpleName}! - (${sEntity.name}, $field)")

      checkFieldConstraints(sEntity, sField, value)
    }
  }

  private fun checkRelationChange(entity: String, rel: String): Pair<SEntity, SRelation> {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity type not found! - ($entity)")
    val sRelation = sEntity.rels[rel] ?: throw Exception("Entity relation not found! - ($entity, $rel)")

    // check model constraints
    if (!sRelation.isInput)
      throw Exception("Invalid input relation! - ($entity, $rel)")

    if (!sRelation.isCollection)
      throw Exception("Relation is not a collection, use 'update' instead! - ($entity, $rel)")

    if (!sRelation.isOpen)
      throw Exception("Relation is not open, cannot change links! - ($entity, $rel)")

    return sEntity to sRelation
  }
}