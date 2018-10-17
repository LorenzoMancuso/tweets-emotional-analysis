import java.io.{File, PrintWriter}
import java.util.Date

import LexicalResPreProcessing.{GetFileList, SparkCount}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD



object LexicalResPreProcessing extends indexes {

  def PreProcessing(sc:SparkContext): List[Feeling] ={
    var feelingList=List[Feeling]()
    //EmoSN, NRC, sentisense, GI_NEG, GI_POS,HL-Negatives,HL-Positives,LIWC-POS,LIWC-NEG,LISTPOSEFFTERMS, LISTNEGEFFTERMS, refused
    var res_type=Array("","","","","","","","","","","","")

    val path:String="./DATASET/Risorse-lessicali/"
    val feelings = GetListOfSubDirectories(path)

    var totalFeelingWords=0
    feelings.foreach{feelingName=>

      if(feelingName!="ConScore"){ //feelings files
        var tmp:Feeling=new Feeling(feelingIndex(),feelingName)
        totalFeelingWords=0

        GetFileList(path+feelingName).foreach{lexicalResource=>
          totalFeelingWords+=SparkCountTotalWords(path+feelingName+"/"+lexicalResource,sc)
          var resType = CheckResType(lexicalResource)

          SparkCount(path+feelingName+"/"+lexicalResource,sc).collect().foreach{x=>
            var tmpLemma=tmp.lemmas.filter(_.name==x._1)

            if(tmpLemma.length>0){//lemma already present
            var tmpLR=tmpLemma.head.lexicalRes.filter(_.name==resType)

              if(tmpLR.length>0){//lr already present
                tmpLR.head.occurrences+=x._2

              }else{//new lr for this lemma
              var lr=new LexicalResource(lexicalResourceIndex(),resType,x._2)
                tmpLemma.head.lexicalRes=lr::tmpLemma.head.lexicalRes

              }
            }else{//new lemma for this feeling
            var lr=new LexicalResource(lexicalResourceIndex(),resType,x._2)
              var lemma=new Lemma(lemmaIndex(),x._1)
              lemma.lexicalRes=lr::lemma.lexicalRes
              tmp.lemmas=lemma::tmp.lemmas
            }
          }
        }
        tmp.totalWords=totalFeelingWords
        feelingList=tmp::feelingList

      }
    }
    SetPercentage(feelingList)
    //CheckScores(feelingList,path,sc)
    PrintResultToFile(feelingList)
    return feelingList
  }

  //print in result document and set percentage
  def SetPercentage(feelingList:List[Feeling]): Unit ={
    var totalLemmaOccurrences:Float=0
    feelingList.foreach(f=>{
      f.lemmas.foreach { l =>
        totalLemmaOccurrences=0
        l.lexicalRes.foreach{lr=>
          totalLemmaOccurrences+=lr.occurrences
        }
        l.percentage=totalLemmaOccurrences/f.totalWords
      }
    })
  }

  def CheckScores(feelingList: List[Feeling], path: String, sc: SparkContext): Unit = {
    val feelingName: String = "ConScore"
    //                  lemma, List[lexicalResName,score]
    var scores=Map[String,Map[String,Double]]()
    GetFileList(path + feelingName).foreach { lexicalResource:String =>
      SparkCount(path + feelingName + "/" + lexicalResource, sc).collect().foreach { x =>
        if(scores.filterKeys(_==x._1).nonEmpty){
          scores(x._1)+=(lexicalResource->x._2)
        }else{
          scores+=(x._1->(lexicalResource->x._2))
        }
      }
    }

    scores.foreach{sc=>
      feelingList.foreach{
        _.lemmas.filter(_.name==sc._1)(0).lexicalRes//in progress
      }
    }
  }

  def CheckResType(lexicalResource: String): String ={
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

  def GetListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  def SparkCount(filename:String, sc:SparkContext): RDD[(String,Int)] ={
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

  def SparkCountTotalWords(filename:String, sc:SparkContext): Int ={
    val text = sc.textFile(filename)
    val counts = text.flatMap(line => line.split(" ")).filter(!_.contains("_")).collect.length
    return counts
  }

  //***HELPER - print in result document and set percentage***
  def PrintResultToFile(feelingList:List[Feeling]): Unit ={
    val pw = new PrintWriter(new File("./RESULTS/result_"+new Date().toString().replaceAll(" ","_")+".txt" ))
    pw.write("print result "+feelingList.length+"\n")
    feelingList.foreach(f=>{
      pw.write(f.id+" "+f.name)
      f.lemmas.foreach { l =>
        pw.write("   "+l.id+" "+l.name+"\n")
        l.lexicalRes.foreach{lr=>
          pw.write("       "+lr.id+" "+lr.name+" "+lr.occurrences+"\n")
        }
        pw.write("          "+l.percentage+"%\n")
      }
    })
    pw.close
  }
}
