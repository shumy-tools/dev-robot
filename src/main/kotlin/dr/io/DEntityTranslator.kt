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
    val (head, tail) = data.unpack
    val root = Insert(table(head.schema), CreateAction(head.schema))
    val all = processEntity(head, tail, root)

    return Instructions(root, all)
  }

  fun update(id: Long, data: DEntity): Instructions {
    val root = Update(table(data.schema), id, UpdateAction(data.schema))
    val (topInclude, bottomInclude) = processUnpackedEntity(data, root)
    val all = topInclude.plus(root).plus(bottomInclude)

    return Instructions(root, all)
  }

  /* ------------------------------------------------------------ private ------------------------------------------------------------ */
  private fun table(sEntity: SEntity) = tableTranslator.getValue(sEntity.name)

  private fun addEntity(entity: DEntity, sRelation: SRelation): Pair<Insert, List<Instruction>> {
    val (head, tail) = entity.unpack
    val rootInst = Insert(table(head.schema), AddAction(head.schema, sRelation))
    return Pair(rootInst, processEntity(head, tail, rootInst))
  }

  private fun processEntity(head: DEntity, tail: List<DEntity>, rootInst: Instruction): List<Instruction> {
    val allInst = mutableListOf<Instruction>()
    val (topInclude, bottomInclude) = processUnpackedEntity(head, rootInst)

    allInst.addAll(topInclude)
    allInst.add(rootInst)
    allInst.addAll(bottomInclude)

    // create extended entities if exist
    var nextEntity = head.schema
    var nextInst = rootInst
    for (item in tail) {
      nextInst = createSubEntity(nextEntity, item, nextInst)
      nextEntity = nextInst.action.sEntity
      allInst.add(nextInst)

      // TODO: add output values?
    }

    return allInst
  }

  private fun createSubEntity(topEntity: SEntity, entity: DEntity, topInst: Instruction): Insert {
    val sEntity = entity.schema
    if (!topEntity.isSealed)
      throw Exception("Not a top @Sealed entity! - (${topEntity.name})")

    topInst.putData("type", sEntity.name)
    return Insert(table(sEntity), CreateAction(sEntity)).apply {
      putRef("ref_super", topInst)
      processUnpackedEntity(entity, this)
    }
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
        val rOutput = topInst.include(entity.allFields, oRef.name)
        topInst.putAllOutput(rOutput)
      } else {
        // A ref_<rel> --> B
        val (root, all) = addEntity(oRef.value, oRef.schema)
        topInclude.addAll(all)
        topInst.putRef("ref__${oRef.name}", root)
        topInst.putOutput(oRef.name, root.output)
      }
    }

    // --------------------------------- allOwnedCollections ----------------------------------
    for (oCol in entity.allOwnedCollections) {
      // A <-- inv_<A>_<rel> B
      val cOutput = mutableListOf<Map<String, Any?>>()
      oCol.value.forEach {
        val (root, all) = addEntity(it, oCol.schema)
        bottomInclude.addAll(all)
        root.putRef("inv_${table(entity.schema)}__${it.name}", topInst)
        cOutput.add(root.output)
      }

      topInst.putOutput(oCol.name, cOutput)
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
        is OneLinkWithoutTraits -> bottomInclude.add(link(entity.schema, lCol.schema, rValue.ref, topInst))

        is OneLinkWithTraits -> {
          val linkInst = link(entity.schema, lCol.schema, rValue.ref.id, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits("traits__${lCol.name}", rValue.ref.traits, linkInst)
        }

        is ManyLinksWithoutTraits -> rValue.refs.forEach {
          bottomInclude.add(link(entity.schema, lCol.schema, it, topInst))
        }

        is ManyLinksWithTraits -> rValue.refs.forEach {
          val linkInst = link(entity.schema, lCol.schema, it.id, topInst)
          bottomInclude.add(linkInst)
          unwrapTraits("traits__${lCol.name}", it.traits, linkInst)
        }

        is OneUnlink -> bottomInclude.add(unlink(entity.schema, lCol.schema, rValue.ref, topInst))

        is ManyUnlink -> rValue.refs.forEach {
          bottomInclude.add(unlink(entity.schema, lCol.schema, it, topInst))
        }
      }
    }

    return Pair(topInclude, bottomInclude)
  }

  private fun link(sEntity: SEntity, sRelation: SRelation, link: Long, topInst: Instruction): Instruction {
    return if (sRelation.isUnique && sRelation.traits.isEmpty()) {
      // A <-- inv_<A>_<rel> B
      Update(table(sRelation.ref), link, LinkAction(sEntity, sRelation)).apply {
        putRef("inv_${table(sEntity)}__${sRelation.name}", topInst)
      }
    } else {
      // A <-- [inv ref] --> B
      Insert("${table(sEntity)}__${sRelation.name}", LinkAction(sEntity, sRelation)).apply {
        putResolvedRef("ref", link)
        putRef("inv", topInst)
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
        putRef("inv", topInst)
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
private fun Instruction.include(fields: List<DField>, at: String? = null): Map<String, Any?> {
  return if (at != null) {
    this.with(at) { this.addFields(fields) }
  } else {
    this.addFields(fields)
  }
}

private fun Instruction.addFields(fields: List<DField>): Map<String, Any?> {
  val output = linkedMapOf<String, Any?>()
  for (field in fields) {
    this.putData(field.name, field.value)
    if (!field.schema.isInput)
      output[field.name] = field.value
  }

  return output
}