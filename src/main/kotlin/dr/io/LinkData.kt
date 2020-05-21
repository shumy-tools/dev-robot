package dr.io

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import dr.schema.RefID
import dr.schema.Traits

const val ONE_UNLINK = "one-unlink"

const val ONE_ADD = "one-add"
const val MANY_ADD = "many-add"
const val ONE_RMV = "one-rmv"
const val MANY_RMV = "many-rmv"

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class LinkData

  sealed class ManyLinks: LinkData()

    @JsonTypeName("many-links")
    data class ManyLinksWithoutTraits(val refs: List<RefID>): ManyLinks() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

    @JsonTypeName("many-links-traits")
    data class ManyLinksWithTraits(val refs: List<Traits>): ManyLinks() {
      constructor(vararg refs: Traits): this(refs.toList())
    }

    @JsonTypeName("many-unlink")
    data class ManyUnlink(val refs: List<RefID>): ManyLinks() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

  sealed class OneLink: LinkData()

    @JsonTypeName("one-link")
    data class OneLinkWithoutTraits(val ref: RefID): OneLink()

    @JsonTypeName("one-link-traits")
    data class OneLinkWithTraits(val ref: Traits): OneLink()

    @JsonTypeName(ONE_UNLINK)
    data class OneUnlink(val ref: RefID): OneLink()


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class OwnData

  sealed class ManyOwns: OwnData()

    @JsonTypeName(MANY_ADD)
    data class ManyAdd(val values: List<DEntity>): ManyOwns() {
      constructor(vararg values: DEntity): this(values.toList())
    }

    @JsonTypeName(MANY_RMV)
    data class ManyRemove(val refs: List<RefID>): ManyOwns() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

  sealed class OneOwn: OwnData()

    @JsonTypeName(ONE_ADD)
    data class OneAdd(val value: DEntity): OneOwn()

    @JsonTypeName(ONE_RMV)
    data class OneRemove(val ref: RefID): OneOwn()
