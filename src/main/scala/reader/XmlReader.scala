package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml._
import java.io._
import java.io.PrintWriter
import scala.io.Source
import scala.util.parsing.json.JSONObject



case class XmlReader(
                    path: String,
                    testPath: String
                    ) extends Reader {
  val format = "json"       // Using temporary file with json extension
//  println(path)

  // Helper function to recursively parse any XML element into a Map
  private def parseXML(node: Node): Map[String, Any] = {
    // Extract the element's tag name and its children
    val tagName = node.label
    val children = node.child.filterNot(_.isInstanceOf[Text])

    // If the node has children, recursively parse them
    if (children.nonEmpty) {
      // If multiple child nodes with the same tag, aggregate them into a list
      val childrenMap = children.groupBy(_.label).map {
        case (key, elems) =>
          // Each child tag gets processed as a list of maps (for repeated elements)
          key -> elems.map(parseXML)
      }

      // Combine tag name with the children map
      Map(tagName -> childrenMap)
    } else {
      // If no children, just return the tag name and its text content
      Map(tagName -> node.text.trim)
    }
  }



  // Function to parse the entire XML and convert it to a Map structure
  private def parseXMLDocument(xml: String): Map[String, Any] = {
    val xmlNode = scala.xml.XML.loadString(xml) // Load the XML as a Node
    parseXML(xmlNode) // Start parsing from the root
  }

  private def cleanMap(m: Map[String, Any]): Any = {
    val allKeys = m.keys.flatMap { key =>
      m.get(key) match {
        case Some(value: Map[_, _]) =>
          // If the value is a map, get its keys and return them
          Some(value.asInstanceOf[Map[String, Any]].keys.toList)
        case _ =>
          // If the value is not a map, return an empty list
          None
      }
    }.toList

    if (m.size == 1 && allKeys.isEmpty) {

      // If the map has only one entry, get the value of that entry
      m.head._2 match {
        case nestedMap: Map[_, _] => cleanMap(nestedMap.asInstanceOf[Map[String, Any]])  // Recursively clean the nested map
        case nestedList: List[_] => nestedList.map {
          case listItem: Map[_, _] => cleanMap(listItem.asInstanceOf[Map[String, Any]])  // Recursively clean each map in the list
          case other => other  // If the item is not a map, leave it as is
        }
        case other => other  // If it's not a map or list, leave it as is
      }
    } else {

      m.map {
        case (key, value) =>
          key -> (value match {
            case nestedMap: Map[_, _] =>
              cleanMap(nestedMap.asInstanceOf[Map[String, Any]])  // Handle nested map
            case nestedList: List[_] =>
              // If the value is a list, recursively clean each element of the list,
              // but maintain the structure of the list
              nestedList.map {
                case listItem: Map[_, _] =>
                  cleanMap(listItem.asInstanceOf[Map[String, Any]])  // Recursively clean each map in the list
                case other => other  // If the item is not a map, leave it as is
              }
            case other => other  // If it's not a map or list, leave it as is
          })
      }
    }
  }


  private def removeMotherTag(m1: Any): Any = {
    // Check if m is a Map
    if (m1.isInstanceOf[Map[_, _]]) {
      val m = m1.asInstanceOf[Map[String, Any]]
      if (m.size == 1) {
        // If the map has only one entry, get the value of that entry
        m.head._2 match {
          case nestedMap: Map[_, _] => cleanMap(nestedMap.asInstanceOf[Map[String, Any]])  // Recursively clean the nested map
          case nestedList: List[_] => nestedList.map {
            case listItem: Map[_, _] => cleanMap(listItem.asInstanceOf[Map[String, Any]])  // Recursively clean each map in the list
            case other => other  // If the item is not a map, leave it as is
          }
          case other => other  // If it's not a map or list, leave it as is
        }
      } else {
        // If the map has more than one entry, recursively clean the map
        m.map {
          case (key, value) =>
            // Recursively apply cleanMap to each value
            key -> (value match {
              case nestedMap: Map[_, _] => cleanMap(nestedMap.asInstanceOf[Map[String, Any]])  // Handle nested map
              case nestedList: List[_] => nestedList.map {
                case listItem: Map[_, _] => cleanMap(listItem.asInstanceOf[Map[String, Any]])  // Recursively clean each map in the list
                case other => other  // If the item is not a map, leave it as is
              }
              case other => other  // If it's not a map or list, leave it as is
            })
        }
      }
    }
  }

  private def removeChildTag(m1: Any): Any = {
    val m = m1.asInstanceOf[List[_]]

    var li = List[Map[String, Any]]()  // Use var to allow reassignment

    m.foreach { element =>
      val h = element.asInstanceOf[Map[String, Any]]

      // Ensure h.values.head is a Map[String, Any]
      h.values.head match {
        case map: Map[String, Any] =>
          li = map :: li  // Prepend the map to the list
        case _ =>
          println("Value is not a Map[String, Any]")
      }
    }
    li
  }


  private def combineTag(extraM: List[Map[String, Any]]): List[Any] = {
    // Combine tags having same name in the same Map
    var mainList: List[Any] = List()

    extraM.foreach { element =>
      var extraM2: Map[String, Any] = Map() // Immutable Map
      element.foreach { el1 =>
        el1._2.asInstanceOf[List[_]].foreach { el2 =>
          if (el2.isInstanceOf[List[_]]) {
            val el2_tmp = el2.asInstanceOf[List[Map[String, Any]]]
            var tmp = el2_tmp.head.head._1 // Assuming one element per map as stated
            var li: List[Any] = List()

            el2_tmp.foreach { el3 =>
              if (el3.head._1 == tmp) {
                li = el3.head._2 :: li
              } else {
                li = List[Any]()
                tmp = el3.head._1
              }
            }

            val m = Map(tmp -> li)
            val h = el1._1
            extraM2 = extraM2 + (h -> (extraM2.getOrElse(h, List()) match {
              case current: List[_] => m :: current
              case _                => List(m)
            }))
          } else {
            if (!extraM2.contains(el1._1)) {
              extraM2 = extraM2 + (el1._1 -> el1._2)
            }
          }
        }
      }
      mainList = extraM2 :: mainList
    }

    mainList
  }



  private def transformToJson(inputMap: Map[String, List[Map[String, List[String]]]]): String = {
    val jsonString = inputMap.map { case (key, listOfMaps) =>
      val listJson = listOfMaps.map { map =>
        map.map {
          case (innerKey, values) if values.length == 1 =>
            s"""\"$innerKey\":\"${values.head}\""""
          case (innerKey, values) =>
            s"""\"$innerKey\":[${values.map(value => s"""\"$value\"""").mkString(",")}]\n"""
        }.mkString("{", ",\n", "}\n")
      }.mkString("[", ",\n", "]\n")

      s"""\"$key\":$listJson"""
    }.mkString("", ",\n", "")

    s"{$jsonString}"
  }



  private def defineListMapString(tag: List[Map[String, List[_]]]): List[Map[String, Any]] = {
    var lr = List[Map[String, Any]]()
    tag.foreach { m =>
      var mapTmp = Map[String, Any]()
      m.foreach { m1 =>
        if (m1._2.head.isInstanceOf[String]) {
          //        println("ok"+m1._2.head)
          mapTmp = mapTmp + (m1._1 -> s"${m1._2.head.toString}")
        }
        else {
          val jT = transformToJson(m1._2.head.asInstanceOf[Map[String, List[Map[String, List[String]]]]])
          //          println(jT+"\n")
          mapTmp = mapTmp + (m1._1 -> jT)
        }
      }
      lr = mapTmp :: lr
    }
    lr
  }


  private def transformToJsonList(data: List[Map[String, Any]]): String = {
    val transformedList = data.map { item =>
      // Convert each key-value pair in the map to strings
      val transformedItem = item.map { case (key, value) =>
        key -> value.toString
      }
      // Convert the transformed map into a JSON object string
      JSONObject(transformedItem).toString()
    }
    // Join all JSON objects into a JSON array
    "[" + transformedList.mkString(",\n") + "]"
  }



  private def writeJsonToFile(json: String, filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    try {
      pw.write(json)
    } finally {
      pw.close()
    }
  }


  def read()(implicit spark: SparkSession): DataFrame = {

    // PREPROCESSING
    val filePath = path // Path to your XML file

    // Read the entire content of the XML file into a string
    val source = Source.fromFile(filePath)
    val xmlString: String = source.mkString
    val parsedMap = parseXMLDocument(xmlString)
//    println(parsedMap+"\n\n")
    val tmp = cleanMap(parsedMap)
//    println(tmp+"\n\n")
    val cleanM = removeMotherTag(tmp)
//    println(cleanM+"\n\n")
    val extraM = removeChildTag(cleanM).asInstanceOf[List[Map[String, Any]]]
//    println(extraM+"\n\n")
    val tag = combineTag(extraM)
//    println(tag+"\n\n")
    val tmp2 = defineListMapString(tag.asInstanceOf[List[Map[String, List[_]]]])
//    println(tmp2+"\n\n")
    val transformed = transformToJsonList(tmp2)
//    println(transformed+"\n\n")


    writeJsonToFile(transformed, testPath)
    source.close()

    val b = true

    spark.read.format(format)
      .option("multiline", b)
      .load(testPath)
  }
}
