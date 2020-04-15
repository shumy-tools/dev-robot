package dr.test

import dr.io.InputProcessor
import dr.schema.SParser
import org.junit.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

private val time = LocalTime.of(10, 30, 20)
private val date = LocalDate.of(2020, 1, 25)
private val datetime = LocalDateTime.of(date, time)
private val schema = SParser.parse(A::class, B::class, B1::class)

class InputProcessorTest {
  private val ip = InputProcessor(schema)

  private val fieldInputs = """{
    "oneText":"one",
    "twoInt":2,
    "threeLong":3,
    "fourFloat":4.0,
    "fiveDouble":5.0,
    "sixBoolean":true,
    "sevenTime":"10:30:20",
    "eightDate":"2020-01-25",
    "nineDateTime":"2020-01-25T10:30:20"
  }"""

  @Test fun testCreateFieldTypes() {
    val entity = ip.create(A::class, fieldInputs)
    assert(entity.toMap() == mapOf(
      "oneText" to "one",
      "twoInt" to 2,
      "threeLong" to 3L,
      "fourFloat" to 4.0F,
      "fiveDouble" to 5.0,
      "sixBoolean" to true,
      "sevenTime" to time,
      "eightDate" to date,
      "nineDateTime" to datetime
    ))
  }

  @Test fun testUpdateFieldTypes() {
    val entity = ip.update(A::class, fieldInputs)
    assert(entity.toMap() == mapOf(
      "oneText" to "one",
      "twoInt" to 2L,
      "threeLong" to 3L,
      "fourFloat" to 4.0,
      "fiveDouble" to 5.0,
      "sixBoolean" to true,
      "sevenTime" to time,
      "eightDate" to date,
      "nineDateTime" to datetime
    ))
  }
}