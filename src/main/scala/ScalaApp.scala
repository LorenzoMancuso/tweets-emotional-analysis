import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.File
// PrintWriter
import java.io._


class LexicalResource(var name:String, var occurrences:Int=1)
class Lemma(var name:String, var lexicalRes:List[LexicalResource]=List[LexicalResource]())
class Feeling(var name:String, var lemmas:List[Lemma]=List[Lemma]())

object ScalaApp {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("MAADB - progetto").setMaster("local[*]").set("spark.executor.memomory","1G"))
    sc.setLogLevel("ERROR")

    var feelingList=List[Feeling]()
    //EmoSN, NRC, sentisense, GI_NEG, GI_POS,HL-Negatives,HL-Positives,LIWC-POS,LIWC-NEG,LISTPOSEFFTERMS, LISTNEGEFFTERMS, refused
    var res_type=Array("","","","","","","","","","","","")

    val path:String="./DATASET/Risorse-lessicali/"
    val feelings = getListOfSubDirectories(path)

    feelings.foreach {feelingName=>
      if(feelingName!="ConScore"){
        var tmp:Feeling=new Feeling(feelingName)

        getFileList(path+feelingName).foreach{lexicalResource=>
          var resType = checkResType(lexicalResource)

          sparkCount(path+feelingName+"/"+lexicalResource,sc).collect().foreach{x=>
            var tmpLemma=tmp.lemmas.filter(_.name==x._1)

            if(tmpLemma.length>0){//lemma already present
              var tmpLR=tmpLemma.head.lexicalRes.filter(_.name==resType)

              if(tmpLR.length>0){//lr already present
                tmpLR.head.occurrences+=x._2

              }else{//new lr for this lemma
                var lr=new LexicalResource(resType,x._2)
                tmpLemma.head.lexicalRes=lr::tmpLemma.head.lexicalRes

              }
            }else{//new lemma for this feeling
              var lr=new LexicalResource(resType,x._2)
              var lemma=new Lemma(x._1)
              lemma.lexicalRes=lr::lemma.lexicalRes
              tmp.lemmas=lemma::tmp.lemmas
            }
          }
        }
        feelingList=tmp::feelingList
      }
    }

    val pw = new PrintWriter(new File("result.txt" ))
    pw.write("print result "+feelingList.length+"\n")
    feelingList.foreach(f=>{
      pw.write(f.name)
      f.lemmas.foreach { l =>
        pw.write("   "+l.name+"\n")
        l.lexicalRes.foreach{lr=>
          pw.write("       "+lr.name+" "+lr.occurrences+"\n")
        }
      }
    })
    pw.close
  }

  def checkResType(lexicalResource: String): String ={
    if(lexicalResource.toLowerCase().contains("emosn")){
      return "EmoSN"

    }else if(lexicalResource.toLowerCase().contains("nrc")){
      return "NRC"

    }else if(lexicalResource.toLowerCase().contains("sentisense")){
      return "sentisense"

    }else if(lexicalResource.toLowerCase().contains("gi_neg")){
      return "GI_NEG"

    }else if(lexicalResource.toLowerCase().contains("gi_pos")){
      return "GI_POS"

    }else if(lexicalResource.toLowerCase().contains("hl-negatives")){
      return "HL-negatives"

    }else if(lexicalResource.toLowerCase().contains("hl-positives")){
      return "HL-positives"

    }else if(lexicalResource.toLowerCase().contains("liwc-pos")){
      return "LIWC-POS"

    }else if(lexicalResource.toLowerCase().contains("liwc-neg")){
      return "LIWC-NEG"

    }else if(lexicalResource.toLowerCase().contains("listposeffterms")){
      return "listPosEffTerms"

    }else if(lexicalResource.toLowerCase().contains("listnegeffterms")){
      return "listNegEffTerms"

    }else{
      return "refused"

    }
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }

  def getFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  def sparkCount(filename:String, sc:SparkContext): RDD[(String,Int)] ={
    println(filename)
    // get threshold
    val threshold = 0
    // read in text file and split each document into words
    val tokenized = sc.textFile(filename).flatMap(_.split(" "))
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _).filter(!_._1.contains("_"))
    println("finish")
    return wordCounts
  }
}
