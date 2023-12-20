import org.ntic.entregable.Time
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TimeTest extends AnyFlatSpec with Matchers {
  "A org.ntic.entregable.Time" should "be correctly initialized from string" in {
    val timeStr1 = "650"
    val timeStr2 = "1440"
    val expected1 = Time(6, 50)
    val expected2 = Time(14, 40)

    val result1 = Time.fromString(timeStr1)
    val result2 = Time.fromString(timeStr2)
    result1 shouldEqual expected1
    result2 shouldEqual expected2
  }

}

