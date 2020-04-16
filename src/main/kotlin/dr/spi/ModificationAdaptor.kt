package dr.spi

import dr.io.Instructions

interface IModificationAdaptor {
  fun commit(instructions: Instructions): Long
}