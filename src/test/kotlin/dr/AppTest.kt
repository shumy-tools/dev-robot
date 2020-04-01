package dr

import kotlin.test.Test
import kotlin.test.assertNotNull

class AppTest {
  @Test fun testAppHasAGreeting() {
    assertNotNull("classUnderTest.greeting", "app should have a greeting")
  }
}
