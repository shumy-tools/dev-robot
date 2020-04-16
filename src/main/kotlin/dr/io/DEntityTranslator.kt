package dr.io

import dr.schema.EntityType
import dr.schema.SEntity
import dr.schema.SRelation
import dr.schema.Schema

class DEntityTranslator(private val schema: Schema) {
  private val tableTranslator = schema.entities.map { (name, _) ->
    name to name.replace('.', '_').toLowerCase()
  }.toMap()


  fun create(data: DEntity): Instructions {
    val (head, tail) = createEntity(data)
    return Instructions(head, tail)
  }

  fun update(id: Long, data: DEntity): Instructions {
    val head = Update(table(data.schema), id, UpdateAction(data.schema))
    val tail = processUnpackedEntity(data, head)
    return Instructions(head, tail)
  }

  /* ------------------------------------------------------------ private ------------------------------------------------------------ */
  private fun table(sEntity: SEntity) = tableTranslator.getValue(sEntity.name)

  private fun createEntity(entity: DEntity): Pair<Insert, List<Insert>> {
    val allInst = mutableListOf<Insert>()
    val (head, tail) = entity.unpack

    val topInst = Insert(table(head.schema), CreateAction(head.schema))
    processUnpackedEntity(head, topInst)

    // create extended entities if exist
    var nextEntity = head.schema
    var nextInst = topInst
    for (item in tail) {
      nextInst = createSubEntity(nextEntity, item, nextInst)
      nextEntity = nextInst.action.sEntity
      allInst.add(nextInst)
    }

    return Pair(topInst, allInst)
  }

  private fun createSubEntity(topEntity: SEntity, entity: DEntity, topInst: Instruction): Insert {
    val sEntity = entity.schema
    if (!topEntity.isSealed)
      throw Exception("Not a top @Sealed entity! - (${topEntity.name})")

    topInst.putData("type", sEntity.name)
    return Insert(table(sEntity), CreateAction(sEntity)).apply {
      putUnresolvedRef("ref_super", topInst)
      processUnpackedEntity(entity, this)
    }
  }

  private fun processUnpackedEntity(entity: DEntity, topInst: Instruction): List<Instruction> {
    val allInst = mutableListOf<Instruction>()

    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    topInst.include(entity.allFields)

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in entity.allOwnedReferences) {
      if (oRef.schema.isUnique || oRef.schema.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        topInst.include(entity.allFields, oRef.name)
      } else {
        // A ref_<rel> --> B
        val (head, tail) = createEntity(oRef.value)
        allInst.include(head, tail)
        topInst.putUnresolvedRef("ref__${oRef.name}", head)
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in entity.allOwnedCollections) {
      // A <-- inv_<A>_<rel> B
      oCol.value.forEach {
        val (head, tail) = createEntity(it)
        allInst.include(head, tail)
        head.putUnresolvedRef("inv_${table(entity.schema)}__${it.name}", topInst)
      }
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in entity.allLinkedReferences) {
      // A ref_<rel> --> B
      when (val rValue = lRef.value) {
        is OneLinkWithoutTraits -> topInst.putResolvedRef("ref__${lRef.name}", rValue.ref)

        is OneLinkWithTraits -> {
          topInst.putResolvedRef("ref__${lRef.name}", rValue.ref.id)
          unwrapTraits("traits__${lRef.name}", rValue.ref.traits, topInst)
        }

        is OneUnlink -> {
          topInst.putResolvedRef("ref__${lRef.name}", null)
          if (lRef.schema.traits.isNotEmpty()) {
            topInst.putData("traits__${lRef.name}", null)
          }
        }
      }
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in entity.allLinkedCollections) {
      when (val rValue = lCol.value) {
        is OneLinkWithoutTraits -> allInst.add(link(entity.schema, lCol.schema, rValue.ref, topInst))

        is OneLinkWithTraits -> {
          val linkInst = link(entity.schema, lCol.schema, rValue.ref.id, topInst)
          allInst.add(linkInst)
          unwrapTraits("traits__${lCol.name}", rValue.ref.traits, linkInst)
        }

        is ManyLinksWithoutTraits -> rValue.refs.forEach {
          allInst.add(link(entity.schema, lCol.schema, it, topInst))
        }

        is ManyLinksWithTraits -> rValue.refs.forEach {
          val linkInst = link(entity.schema, lCol.schema, it.id, topInst)
          allInst.add(linkInst)
          unwrapTraits("traits__${lCol.name}", it.traits, linkInst)
        }

        is OneUnlink -> allInst.add(unlink(entity.schema, lCol.schema, rValue.ref, topInst))

        is ManyUnlink -> rValue.refs.forEach {
          allInst.add(unlink(entity.schema, lCol.schema, it, topInst))
        }
      }
    }

    return allInst
  }

  private fun link(sEntity: SEntity, sRelation: SRelation, link: Long, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(table(sRelation.ref), link, LinkAction(sEntity, sRelation)).apply {
        putUnresolvedRef("inv_${table(sEntity)}__${sRelation.name}", topInst)
      }
    } else {
      // A <-- [inv ref] --> B
      Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", link)
        putUnresolvedRef("inv", topInst)
      }
    }
  }

  private fun unlink(sEntity: SEntity, sRelation: SRelation, link: Long, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(table(sRelation.ref), link, UnlinkAction(sEntity, sRelation)).apply {
        putResolvedRef("inv_${table(sEntity)}__${sRelation.name}", null)
      }
    } else {
      // A <-- [inv ref] --> B
      Delete("${table(sEntity)}__${sRelation.name}", UnlinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", link)
        putUnresolvedRef("inv", topInst)
      }
    }
  }

  private fun unwrapTraits(at: String, traits: List<Any>, topInst: Instruction) {
    for (trait in traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      val dTrait = DEntity(sTrait, cEntity = trait)

      /*
      topInst.with(true, at) {
        processUnpackedEntity(dTrait, topInst)
      }*/

      // A <fields>
      topInst.include(dTrait.allFields, at)

      // TODO: process references!
    }
  }
}

/* ------------------------- helpers -------------------------*/
private fun Instruction.include(fields: List<DField>, at: String? = null) {
  this.with(at != null, at!!) {
    for (field in fields)
      this.putData(field.name, field.value)
  }
}

private fun MutableList<Instruction>.include(head: Instruction, tail: List<Instruction>): MutableList<Instruction> {
  this.add(head)
  this.addAll(tail)
  return this
}