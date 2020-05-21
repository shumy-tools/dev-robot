package dr.io

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import dr.schema.RefID
import dr.schema.Traits

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

    @JsonTypeName("one-unlink")
    data class OneUnlink(val ref: RefID): OneLink()


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
sealed class OwnData

  sealed class ManyOwns: OwnData()

    @JsonTypeName("many-add")
    data class ManyAdd(val values: List<DEntity>): ManyOwns() {
      constructor(vararg values: DEntity): this(values.toList())
    }

    @JsonTypeName("many-rmv")
    data class ManyRemove(val refs: List<RefID>): ManyOwns() {
      constructor(vararg refs: RefID): this(refs.toList())
    }

  sealed class OneOwn: OwnData()

    @JsonTypeName("one-add")
    data class OneAdd(val value: DEntity): OneOwn()

    @JsonTypeName("one-rmv")
    data class OneRemove(val ref: RefID): OneOwn()
