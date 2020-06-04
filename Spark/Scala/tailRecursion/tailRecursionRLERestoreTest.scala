import com.jentekco.scala.tailRecursion
class tailRecursionRLERestoreTest extends org.scalatest.FunSuite {
  test("tailRecursion.rleRestore") {
    assert(tailRecursion.rleRestore(tailRecursion.runLevelEncode("Get Uppppppppppppppppppppppppppppppppppppp!"))=="Get Uppppppppppppppppppppppppppppppppppppp!")
  }
}
