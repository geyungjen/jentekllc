package com.jentekco.scala
//George Jen
//Jen Tek LLC
//20200603
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

object tailRecursion {
  //var result=List()

//This is to reverse List[Any], reinventing wheel of List().reverse method  
  
def reverseList(l:List[Any]): List[Any]={
    var result:List[Any]=List()
//Inner function that is tail recursive
    def doReverse(l: List[Any]): List[Any]={
    
    
    if (l.size<1)
        result
    else
       {
           result = l(0) :: result
//           println(result)
           doReverse(l.drop(1))
       }
}
return doReverse(l)
}
  
  

 def runLevelEncode(l: String): String = { 
    var stringBuf:ListBuffer[String]=ListBuffer.empty[String]
    stringBuf.clear()
//Inner helper function that is tail recursion
    def doRLE(l: String): String = {
    
    l.toList match {
    case Nil=>scala.collection.mutable.ListBuffer[String]()
    case h :: t => {
        
    val ms = l.toList.span(_ == h)

    if (ms._1.size>1)
        {
//       print(ms._1.size.toString+ms._1(0))
        stringBuf += ms._1.size.toString+ms._1(0)
        }
    else
        {
 //       print(ms._1(0))
        stringBuf += ""+ms._1(0)
        }
    doRLE(ms._2.mkString)
                   }
    
  }
  return stringBuf.mkString                                          
}
return doRLE(l)
}


def rleRestore(l:String):String={
    var stringBuf:ListBuffer[String]=ListBuffer.empty[String]
    var digitArray:ArrayBuffer[Int]=ArrayBuffer[Int]()
    stringBuf.clear()
    digitArray.clear()
    for (i<-l)
{
    if (i.isDigit)
      {
       digitArray += i.toString.toInt
      }
    else
      {
      if (!digitArray.isEmpty)
          for (_<-0 until digitArray.mkString.toInt)
              {
//              print(i)
              stringBuf += ""+i
              }
      else
          {
//              print(i)
              stringBuf += ""+i
          }
      digitArray.clear()
      }
      
}

    
    
    return stringBuf.mkString
}
def main(args: Array[String]): Unit = {
// Do reverse a list
   println("Do List Reverse")
   val originalList=List((1,2),3,"abc",3.1416)
   val reversedList:List[Any]=reverseList(originalList)
   println(s"Original List is: $originalList")
   println(s"Reversed List is: $reversedList")
  
   println("")
   println("Do String Run Level Encoding")
   val x="Get Uppppppppppppppppppppppppppppppppppppp!"
   val rleString=runLevelEncode(x)
   val originalString=rleRestore(rleString)
   println(s"Original String: $x")
   println(s"RLE Compressed: $rleString")
   println(s"Restored String: $originalString")
   
}
}

