package dr.io

import dr.io.InstructionType.*
import dr.schema.*
import dr.schema.tabular.*

class InstructionBuilder(private val tables: Tables) {
  fun create(entity: DEntity): Instructions {
    val (head, tail) = entity.unpack
    val rootInst = Insert(entity.refID, tables.get(head.schema), CREATE)
    val all = processEntity(head, tail, rootInst)

    return Instructions(all).apply {
      root = rootInst
    }
  }

  fun update(entity: DEntity): Instructions {
    val rootInst = Update(entity.refID, tables.get(entity.schema), UPDATE)
    val (topInclude, bottomInclude) = processUnpackedEntity(entity, rootInst)
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
    val rootInst = Insert(entity.refID, tables.get(head.schema), ADD)
    return Pair(rootInst, processEntity(head, tail, rootInst))
  }

  private fun processEntity(head: DEntity, tail: List<DEntity>, rootInst: Instruction): MutableList<Instruction> {
    val allInst = mutableListOf<Instruction>()
    val (topInclude, bottomInclude) = processUnpackedEntity(head, rootInst)

    allInst.addAll(topInclude)
    allInst.add(rootInst)
    allInst.addAll(bottomInclude)

    // create extended entities if exist
    var topInst = rootInst
    for (item in tail) {
      val topEntity = item.dSuper.schema
      topInst = Insert(item.refID, tables.get(item.schema), CREATE).apply {
        putRef(TSuperRef(topEntity), topInst.refID)
        val (subTopInclude, subBottomInclude) = processUnpackedEntity(item, this)
        allInst.addAll(subTopInclude)
        allInst.add(this)
        allInst.addAll(subBottomInclude)
      }

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
    for (oRef in entity.allOwnedReferences.filter { it.name != SUPER && it.name != PARENT }) {
      when (val rValue = oRef.value) {
        is OneAdd -> if (oRef.schema.ref.type == EntityType.TRAIT) {
          // A {<rel>: <fields>}
          // TODO: not tested
          val rOutput = topInst.include(rValue.value.allFields, oRef.schema)
          topInst.putAllOutput(rOutput)
        } else {
          // A ref_<rel> --> B
          val (root, all) = addEntity(rValue.value)
          topInclude.addAll(all)
          topInst.putRef(TDirectRef(entity.schema, oRef.schema), root.refID)
          topInst.putOutput(oRef.name, root.output)
        }

        is OneRemove -> if (oRef.schema.ref.type == EntityType.TRAIT) {
          // A {<rel>: <fields>}
          // TODO: not tested
          topInst.with(TEmbedded(oRef.schema, oRef.schema.ref)) {
            oRef.schema.ref.fields.values.forEach { sField ->
              topInst.putData(TField(sField), null)
            }
          }
        } else {
          // A ref_<rel> --> B
          topInst.putRef(TDirectRef(entity.schema, oRef.schema), RefID())
        }
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in entity.allOwnedCollections) {
      // A <-- inv_<A>_<rel> B
      val cOutput = mutableListOf<Map<String, Any?>>()
      topInst.putOutput(oCol.name, cOutput)
      when (val rValue = oCol.value) {
        is OneAdd -> {
          val (root, all) = addEntity(rValue.value)
          bottomInclude.addAll(all)
          root.putRef(TInverseRef(entity.schema, oCol.schema), topInst.refID)
          cOutput.add(root.output)
        }

        is ManyAdd -> {
          rValue.values.forEach {
            val (root, all) = addEntity(it)
            bottomInclude.addAll(all)
            root.putRef(TInverseRef(entity.schema, oCol.schema), topInst.refID)
            cOutput.add(root.output)
          }
        }

        is OneRemove -> {
          bottomInclude.add(Update(rValue.ref, tables.get(oCol.schema.ref), InstructionType.REMOVE).apply {
            putRef(TInverseRef(entity.schema, oCol.schema), RefID())
          })
        }

        is ManyRemove -> {
          rValue.refs.forEach {
            bottomInclude.add(Update(it, tables.get(oCol.schema.ref), InstructionType.REMOVE).apply {
              putRef(TInverseRef(entity.schema, oCol.schema), RefID())
            })
          }
        }
      }
    }

    // --------------------------------- allLinkedReferences ----------------------------------
    for (lRef in entity.allLinkedReferences) {
      // A ref_<rel> --> B
      when (val rValue = lRef.value) {
        is OneLinkWithoutTraits -> topInst.putRef(TDirectRef(entity.schema, lRef.schema), rValue.ref)

        is OneLinkWithTraits -> {
          topInst.putRef(TDirectRef(entity.schema, lRef.schema), rValue.ref.id)
          unwrapTraits(lRef.schema, rValue.ref.traits, topInst)
        }

        is OneUnlink -> {
          topInst.putRef(TDirectRef(entity.schema, lRef.schema), RefID())
          lRef.schema.traits.values.forEach {
            topInst.putData(TEmbedded(lRef.schema, it), null)
          }
        }
      }
    }

    // --------------------------------- allLinkedCollections ---------------------------------
    for (lCol in entity.allLinkedCollections) {
      when (val rValue = lCol.value) {
        is OneLinkWithoutTraits -> bottomInclude.add(link(entity, lCol.schema, rValue.ref, topInst))

        is OneLinkWithTraits -> {
          val linkInst = link(entity, lCol.schema, rValue.ref.id, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits(lCol.schema, rValue.ref.traits, linkInst)
        }

        is ManyLinksWithoutTraits -> rValue.refs.forEach {
          bottomInclude.add(link(entity, lCol.schema, it, topInst))
        }

        is ManyLinksWithTraits -> rValue.refs.forEach {
          val linkInst = link(entity, lCol.schema, it.id, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits(lCol.schema, it.traits, linkInst)
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
      Update(link, tables.get(sRelation.ref), LINK).apply {
        putRef(TInverseRef(entity.schema, sRelation), topInst.refID)
      }
    } else {
      // A <-- [inv ref] --> B
      Insert(RefID(), tables.get(entity.schema, sRelation), LINK).apply {
        putRef(TDirectRef(sRelation.ref, sRelation, false), link)
        putRef(TInverseRef(entity.schema, sRelation, false), topInst.refID)
      }
    }
  }

  private fun unlink(entity: DEntity, sRelation: SRelation, link: RefID, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(link, tables.get(sRelation.ref), UNLINK).apply {
        putRef(TInverseRef(entity.schema, sRelation), RefID())
      }
    } else {
      // A <-- [inv ref] --> B
      Delete(RefID(), tables.get(entity.schema, sRelation), UNLINK).apply {
        putRef(TDirectRef(sRelation.ref, sRelation, false), link)
        putRef(TInverseRef(entity.schema, sRelation, false), topInst.refID)
      }
    }
  }

  private fun unwrapTraits(sRelation: SRelation, traits: List<Any>, topInst: Instruction) {
    for (trait in traits) {
      val name = trait.javaClass.kotlin.qualifiedName
      val sTrait = sRelation.traits[name] ?: throw Exception("Trait type not found! - ($name)")
      //val sTrait = tables.schema.traits[name] ?: throw Exception("Trait type not found! - ($name)")

      /*
      topInst.with(true, at) {
        processUnpackedEntity(dTrait, topInst)
      }*/

      topInst.putData(TEmbedded(sRelation, sTrait), trait)

      // TODO: process references!
    }
  }
}

/* ------------------------- helpers -------------------------*/
private fun Instruction.include(fields: List<DField>, at: SRelation? = null): Map<String, Any?> {
  return if (at != null) {
    with(TEmbedded(at, at.ref)) { addFields(fields) }
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