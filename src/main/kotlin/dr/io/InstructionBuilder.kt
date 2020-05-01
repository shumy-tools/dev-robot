package dr.io

import dr.schema.*
import dr.schema.ActionType.*
import dr.schema.tabular.*

class InstructionBuilder(private val tables: Tables) {

  fun create(data: DEntity): Instructions {
    val (head, tail) = data.unpack
    val rootInst = Insert(tables.get(head.schema), CREATE)
    val all = processEntity(head, tail, rootInst)

    return Instructions(all).apply {
      root = rootInst
    }
  }

  fun update(id: Long, data: DEntity): Instructions {
    val rootInst = Update(tables.get(data.schema), id, UPDATE)
    val (topInclude, bottomInclude) = processUnpackedEntity(data, rootInst)
    val all = mutableListOf(*topInclude.toTypedArray()).apply {
      add(rootInst)
      addAll(bottomInclude)
    }

    return Instructions(all).apply {
      root = rootInst
    }
  }

  /* ------------------------------------------------------------ private ------------------------------------------------------------ */
  private fun addEntity(entity: DEntity): Pair<Insert, List<Instruction>> {
    val (head, tail) = entity.unpack
    val rootInst = Insert(tables.get(head.schema), ADD)
    return Pair(rootInst, processEntity(head, tail, rootInst))
  }

  private fun processEntity(head: DEntity, tail: List<DEntity>, rootInst: Instruction): MutableList<Instruction> {
    val allInst = mutableListOf<Instruction>()
    val (topInclude, bottomInclude) = processUnpackedEntity(head, rootInst)

    allInst.addAll(topInclude)
    allInst.add(rootInst)
    allInst.addAll(bottomInclude)

    // create extended entities if exist
    var topEntity = head.schema
    var topInst = rootInst
    for (item in tail) {
      if (!topEntity.isSealed)
        throw Exception("Not a top @Sealed entity! - (${topEntity.name})")

      topInst.putData(TType, topEntity.name)
      topInst = Insert(tables.get(item.schema), CREATE).apply {
        putRef(TSuperRef(topEntity), topInst)
        val (subTopInclude, subBottomInclude) = processUnpackedEntity(item, this)
        allInst.addAll(subTopInclude)
        allInst.add(this)
        allInst.addAll(subBottomInclude)
      }

      topEntity = item.schema

      // TODO: add output values?
    }

    return allInst
  }

  private fun processUnpackedEntity(entity: DEntity, topInst: Instruction): Pair<List<Instruction>, List<Instruction>> {
    val topInclude = mutableListOf<Instruction>()
    val bottomInclude = mutableListOf<Instruction>()

    // --------------------------------- fields ---------------------------------------------
    // A <fields>
    val fOutput = topInst.include(entity.allFields)
    topInst.putAllOutput(fOutput)

    // --------------------------------- allOwnedReferences ----------------------------------
    for (oRef in entity.allOwnedReferences) {
      if (oRef.schema.isUnique || oRef.schema.ref.type == EntityType.TRAIT) {
        // A {<rel>: <fields>}
        val rOutput = topInst.include(entity.allFields, oRef.schema.ref)
        topInst.putAllOutput(rOutput)
      } else {
        // A ref_<rel> --> B
        val (root, all) = addEntity(oRef.value)
        topInclude.addAll(all)
        topInst.putRef(TDirectRef(entity.schema, oRef.schema), root)
        topInst.putOutput(oRef.name, root.output)
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in entity.allOwnedCollections) {
      // A <-- inv_<A>_<rel> B
      val cOutput = mutableListOf<Map<String, Any?>>()
      oCol.value.forEach {
        val (root, all) = addEntity(it)
        bottomInclude.addAll(all)
        root.putRef(TInverseRef(entity.schema, oCol.schema), topInst)
        cOutput.add(root.output)
      }

      topInst.putOutput(oCol.name, cOutput)
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in entity.allLinkedReferences) {
      // A ref_<rel> --> B
      when (val rValue = lRef.value) {
        is OneLinkWithoutTraits -> topInst.putResolvedRef(TDirectRef(entity.schema, lRef.schema), rValue.ref)

        is OneLinkWithTraits -> {
          topInst.putResolvedRef(TDirectRef(entity.schema, lRef.schema), rValue.ref.refID)
          unwrapTraits(rValue.ref.traits, topInst)
        }

        is OneUnlink -> {
          topInst.putResolvedRef(TDirectRef(entity.schema, lRef.schema), RefID())
          lRef.schema.traits.values.forEach {
            // TODO: is this correct (needs test)
            topInst.putData(TTraits(it), null)
          }
        }
      }
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in entity.allLinkedCollections) {
      when (val rValue = lCol.value) {
        is OneLinkWithoutTraits -> bottomInclude.add(link(entity, lCol.schema, rValue.ref, topInst))

        is OneLinkWithTraits -> {
          val linkInst = link(entity, lCol.schema, rValue.ref.refID, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits(rValue.ref.traits, linkInst)
        }

        is ManyLinksWithoutTraits -> rValue.refs.forEach {
          bottomInclude.add(link(entity, lCol.schema, it, topInst))
        }

        is ManyLinksWithTraits -> rValue.refs.forEach {
          val linkInst = link(entity, lCol.schema, it.refID, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits(it.traits, linkInst)
        }

        is OneUnlink -> bottomInclude.add(unlink(entity, lCol.schema, rValue.ref, topInst))

        is ManyUnlink -> rValue.refs.forEach {
          bottomInclude.add(unlink(entity, lCol.schema, it, topInst))
        }
      }
    }

    return Pair(topInclude, bottomInclude)
  }

  private fun link(entity: DEntity, sRelation: SRelation, link: RefID, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(tables.get(sRelation.ref), link.id, LINK).apply {
        putRef(TInverseRef(entity.schema, sRelation), topInst)
      }
    } else {
      // A <-- [inv ref] --> B
      Insert(tables.get(entity.schema, sRelation), LINK).apply {
        putResolvedRef(TDirectRef(sRelation.ref, sRelation, false), link)
        putRef(TInverseRef(entity.schema, sRelation, false), topInst)
      }
    }
  }

  private fun unlink(entity: DEntity, sRelation: SRelation, link: RefID, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(tables.get(sRelation.ref), link.id, UNLINK).apply {
        putResolvedRef(TInverseRef(entity.schema, sRelation), RefID())
      }
    } else {
      // A <-- [inv ref] --> B
      Delete(tables.get(entity.schema, sRelation), UNLINK).apply {
        putResolvedRef(TDirectRef(sRelation.ref, sRelation, false), link)
        putRef(TInverseRef(entity.schema, sRelation, false), topInst)
      }
    }
  }

  private fun unwrapTraits(traits: List<Any>, topInst: Instruction) {
    for (trait in traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = tables.schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      /*
      topInst.with(true, at) {
        processUnpackedEntity(dTrait, topInst)
      }*/

      topInst.putData(TTraits(sTrait), trait)

      // TODO: process references!
    }
  }
}

/* ------------------------- helpers -------------------------*/
private fun Instruction.include(fields: List<DField>, at: SEntity? = null): Map<String, Any?> {
  return if (at != null) {
    with(TTraits(at)) { addFields(fields) }
  } else {
    addFields(fields)
  }
}

private fun Instruction.addFields(fields: List<DField>): Map<String, Any?> {
  val output = linkedMapOf<String, Any?>()
  for (field in fields) {
    putData(TField(field.schema), field.value)
    if (!field.schema.isInput)
      output[field.name] = field.value
  }

  return output
}