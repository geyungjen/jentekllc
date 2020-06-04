import com.jentekco.scala.tailRecursion
class tailRecursionRLETest extends org.scalatest.FunSuite {
  test("tailRecursion.runLevelEncode") {
    assert(tailRecursion.runLevelEncode("Get Uppppppppppppppppppppppppppppppppppppp!")=="Get U37p!")
  }
}
