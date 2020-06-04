import com.jentekco.scala.tailRecursion
class tailRecursionReverseTest extends org.scalatest.FunSuite {
  test("tailRecursion.reverseList") {
    assert(tailRecursion.reverseList(List(1,2,3,4,5)) === List(5,4,3,2,1))
  }
}
