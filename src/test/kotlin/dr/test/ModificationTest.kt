package dr.test

import dr.schema.Detail
import dr.schema.Master
import dr.schema.SParser
import kotlin.test.Test

class AppTest {
  @Test fun testAppHasAGreeting() {
    @Master class A()
    @Detail class B()

    val mEngine = TestHelper.modification(A::class)

    assert(false)
  }
}
