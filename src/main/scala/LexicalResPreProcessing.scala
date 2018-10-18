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
              var tmpLR=scala.collection.mutable.Map(tmpLemma.head.lexicalRes.filterKeys(_==resType).toSeq:_*)

              if(tmpLR.nonEmpty){//lr already present
                tmpLR(resType)=tmpLR(resType)+x._2

              }else{//new lexical res for this lemma
                tmpLemma.head.lexicalRes+=(resType->x._2)
              }
            }else{//new lemma for this feeling
              var lemma=new Lemma(lemmaIndex(),x._1)
              lemma.lexicalRes+=(resType->x._2)
              tmp.lemmas=lemma::tmp.lemmas
            }
          }
        }
        tmp.totalWords=totalFeelingWords
        feelingList=tmp::feelingList

      }
    }
    SetPercentage(feelingList)
    CheckScores(feelingList,path,sc)
    PrintResultToFile(feelingList)
    return feelingList
  }

  //print in result document and set percentage
  def SetPercentage(feelingList:List[Feeling]): Unit ={
    var totalLemmaOccurrences:Double=0
    feelingList.foreach(f=>{
      f.lemmas.foreach { l =>
        totalLemmaOccurrences=0
        l.lexicalRes.foreach{lr=>
          totalLemmaOccurrences=totalLemmaOccurrences+lr._2
        }
        l.percentage=totalLemmaOccurrences/f.totalWords
      }
    })
  }

  def CheckScores(feelingList: List[Feeling], path: String, sc: SparkContext): Unit = {
    val feelingName: String = "ConScore"
    //                  lemma, List[lexicalResName,score]
    var scores=scala.collection.mutable.Map[String,Map[String,Double]]()
    GetFileList(path + feelingName).foreach { lexicalResource:String =>
      var nameSplit = lexicalResource.replaceAll("(\\.[^\\.]*$)", "")
      SparkCount(path + feelingName + "/" + lexicalResource, sc).collect().foreach { x =>
        var split = x._1.split("\t")
        if(scores.filterKeys(_==x._1).nonEmpty){
          scores(split(0))+=(nameSplit->split(1).toDouble)
        }else{
          scores+=(split(0)->Map(nameSplit->split(1).toDouble))
        }
      }
    }

    scores.foreach{sc=>
      feelingList.foreach{f=>
        var tmp=f.lemmas.filter(_.name==sc._1)
        if(tmp.length>0){
          tmp(0).lexicalRes++=sc._2
        }
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
          pw.write("       "+lr._1+" "+lr._2+"\n")
        }
        pw.write("          "+l.percentage+"%\n")
      }
    })
    pw.close
  }
}
