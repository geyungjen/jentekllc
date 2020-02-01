package com.jentekco.enrichJson

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write


object NoSpark {
  implicit val formats = org.json4s.DefaultFormats
  def main(args: Array[String]): Unit = {
      val rec1: String = """{
    "visitorId": "v1",
    "products": [{
         "id": "i1",
         "interest": 0.68
    }, {
         "id": "i2",
         "interest": 0.42
    }]
}"""
      
      val rec2: String = """{
    "visitorId": "v2",
    "products": [{
         "id": "i1",
         "interest": 0.78
    }, {
         "id": "i3",
         "interest": 0.11
    }]
}"""
      
      val visitsData: Seq[String] = Seq(rec1, rec2)
      for (i<-0 until visitsData.size)
      {
        println(visitsData(i))
        println(" ")
      }

      val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")
      
      case class v_rec(
    id: String,
    interest: Double
    )
    case class p_rec(
        visitorId: String, products: Array[v_rec]
    )
    
 // New case class

    case class v_rec_new(
        id: String,
        name: String,
        interest: Double
      )
    case class p_rec_new(
        visitorId: String, products: Array[v_rec_new]
      )
   
   var jString: Array[String]=Array[String]() 
   var enrichedJson:Array[String]=Array[String]()
   
   for (js<-visitsData)
    {
      var jObj=parse(js)
      var eJ=jObj.extract[p_rec]
      
      var jStringJ=parse(rec1)
      for (i<-0 until eJ.products.size)
       {
           var prodName:String="Invalid Product"
           //if there is no such product, show Invalid Product
           if (productIdToNameMap contains (eJ.products(i).id.toString))                
               prodName=productIdToNameMap(eJ.products(i).id.toString)
           var newRec=p_rec_new(
           visitorId=eJ.visitorId,
           products=Array(v_rec_new(
           eJ.products(i).id.toString,
           prodName,
           eJ.products(i).interest         
           )
           )
           )   
           
//           println(newRec.visitorId, newRec.products(0).name)
           //Now Json Serilizing it
 
           val newRecStr = write(newRec)
//           println(newRecStr)
           jString:+=newRecStr
       }
//      println(jString.size)
      
//      var jStringJ:Array[JObject]=Array[JObject]()

      for (x<-0 until jString.size)
      {   
          if (x==0)
            jStringJ=parse(jString(x))
          else
          {
            jStringJ=jStringJ merge parse(jString(x))
          }
 
      }

//      println("test",jStringJ)
      enrichedJson:+=write(jStringJ)        
      jString=Array[String]()
      
      
    }  
     for (i<-enrichedJson)
        println(i)
  }  

}