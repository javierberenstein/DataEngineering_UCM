import org.ntic.entregable.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class FileUtilsTest extends AnyFlatSpec with Matchers {
  "A line from a file" should "be invalid if there are empty columns" in {
    val singleRow =  """7/1/2023 12:00:00 AM;10257;ALB;Albany, NY;NY;11278;DCA;Washington, DC;VA;;;;"""
    FileUtils.isInvalid(singleRow) shouldBe true
  }

  "A line from a file" should "be valid if there are not any empty columns" in {
    val singleRow = """7/1/2023 12:00:00 AM;10257;ALB;Albany, NY;NY;11057;CLT;Charlotte, NC;NC;-9.00;1320;-25.00"""
    FileUtils.isInvalid(singleRow) shouldBe false
  }

}
