import java.io.File

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.vdurmont.emoji._
object TweetsPreProcessing{

  var punctuaction = List("*","\"","'","\n",">", "<", "=", "+", "_","-", "&", ")", "(", "/", "\\", ":", ";", ".", "!", "?", ",","^","~","”")
  var slang = Map[String, String]("afaik"->"as far as i know","afk"->"away from keyboard","asap"->"as soon as possible","atk"->"at the keyboard","atm"->"at the moment","a3"->"anytime, anywhere, anyplace","bak"->"back at keyboard","bbl"->"be back later","bbs"->"be back soon","bfn"->"bye for now","b4n"->"bye for now","brb"->"be right back","brt"->"be right there","btw"->"by the way","b4n"->"bye for now","cu"->"see you","cul8r"->"see you later","cya"->"see you","faq"->"frequently asked questions","fc"->"fingers crossed","fwiw"->"for what it\"s worth","fyi"->"for your information","gal"->"get a life","gg"->"good game","gmta"->"great minds think alike","gr8"->"great!","g9"->"genius","ic"->"i see","icq"->"i seek you","ilu"->"ilu:i love you","imho"->"in my honest opinion","imo"->"in my opinion","iow"->"in other words","irl"->"in real life","kiss"->"keep it simple, stupid","ldr"->"long distance relationship","lmao"->"laugh my a.. off","lol"->"laughing out loud","ltns"->"long time no see","l8r"->"later","mte"->"my thoughts exactly","m8"->"mate","nrn"->"no reply necessary","oic"->"oh i see","pita"->"pain in the a..","prt"->"party","prw"->"parents are watching","qpsa?"->"que pasa?","rofl"->"rolling on the floor laughing","roflol"->"rolling on the floor laughing out loud","rotflmao"->"rolling on the floor laughing my a.. off","sk8"->"skate","stats"->"your sex and age","asl"->"age, sex, location","thx"->"thank you","ttfn"->"ta-ta for now!","ttyl"->"talk to you later","u"->"you","u2"->"you too","u4e"->"yours for ever","wb"->"welcome back","wtf"->"what the f...","wtg"->"way to go!","wuf"->"where are you from?","w8"->"wait...","7k"->"sick:-d laugher")
  var stopWords = List("a","able","about","above","abst","accordance","according","accordingly","across","act","actually","added","adj","affected","affecting","affects","after","afterwards","again","against","ah","all","almost","alone","along","already","also","although","always","am","among","amongst","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","apparently","approximately","are","aren","arent","arise","around","as","aside","ask","asking","at","auth","available","away","awfully","b","back","be","became","because","become","becomes","becoming","been","before","beforehand","begin","beginning","beginnings","begins","behind","being","believe","below","beside","besides","between","beyond","biol","both","brief","briefly","but","by","c","ca","came","can","cannot","can't","cause","causes","certain","certainly","co","com","come","comes","contain","containing","contains","could","couldnt","d","date","did","didn't","different","do","does","doesn't","doing","done","don't","down","downwards","due","during","e","each","ed","edu","effect","eg","eight","eighty","either","else","elsewhere","end","ending","enough","especially","et","et-al","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","except","f","far","few","ff","fifth","first","five","fix","followed","following","follows","for","former","formerly","forth","found","four","from","further","furthermore","g","gave","get","gets","getting","give","given","gives","giving","go","goes","gone","got","gotten","h","had","happens","hardly","has","hasn't","have","haven't","having","he","hed","hence","her","here","hereafter","hereby","herein","heres","hereupon","hers","herself","hes","hi","hid","him","himself","his","hither","home","how","howbeit","however","hundred","i","id","ie","if","i'll","im","immediate","immediately","importance","important","in","inc","indeed","index","information","instead","into","invention","inward","is","isn't","it","itd","it'll","its","itself","i've","j","just","k","keep	","keeps","kept","kg","km","know","known","knows","l","largely","last","lately","later","latter","latterly","least","less","lest","let","lets","like","liked","likely","line","little","'ll","look","looking","looks","ltd","m","made","mainly","make","makes","many","may","maybe","me","mean","means","meantime","meanwhile","merely","mg","might","million","miss","ml","more","moreover","most","mostly","mr","mrs","much","mug","must","my","myself","n","na","name","namely","nay","nd","near","nearly","necessarily","necessary","need","needs","neither","never","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","now","nowhere","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","omitted","on","once","one","ones","only","onto","or","ord","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","owing","own","p","page","pages","part","particular","particularly","past","per","perhaps","placed","please","plus","poorly","possible","possibly","potentially","pp","predominantly","present","previously","primarily","probably","promptly","proud","provides","put","q","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","respectively","resulted","resulting","results","right","run","s","said","same","saw","say","saying","says","sec","section","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sent","seven","several","shall","she","shed","she'll","shes","should","shouldn't","show","showed","shown","showns","shows","significant","significantly","similar","similarly","since","six","slightly","so","some","somebody","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","still","stop","strongly","sub","substantially","successfully","such","sufficiently","suggest","sup","sure	","t","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that'll","thats","that've","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","thered","therefore","therein","there'll","thereof","therere","theres","thereto","thereupon","there've","these","they","theyd","they'll","theyre","they've","think","this","those","thou","though","thoughh","thousand","throug","through","throughout","thru","thus","til","tip","to","together","too","took","toward","towards","tried","tries","truly","try","trying","ts","twice","two","u","un","under","unfortunately","unless","unlike","unlikely","until","unto","up","upon","ups","us","use","used","useful","usefully","usefulness","uses","using","usually","v","value","various","'ve","very","via","viz","vol","vols","vs","w","want","wants","was","wasnt","way","we","wed","welcome","we'll","went","were","werent","we've","what","whatever","what'll","whats","when","whence","whenever","where","whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether","which","while","whim","whither","who","whod","whoever","whole","who'll","whom","whomever","whos","whose","why","widely","willing","wish","with","within","without","wont","words","world","would","wouldnt","www","x","y","yes","yet","you","youd","you'll","your","youre","yours","yourself","yourselves","you've","z","zero")
  var negationWords = List("not","rather","hardly","couldn\'t","wasn\'t","didn\'t","wouldn\'t","shouldn\'t","weren\'t","don\'t","doesn\'t","haven\'t","hasn\'t","won\'t","wont","hadn\'t")

  var posemoticons = List("B-)",":)", ":-)", ":')", ":'-)", ":D", ":-D",":'-)",":')",":o)",":]",":3",":c)",":>","=]","8)","=)",":}",":^)","8-D","8D","x-D","xD","X-D","XD","=-D","=D","=-3","=3","B^D",":-))",":*",":^*","( '}{' )","^^","(^_^)","^-^","^.^","^3^","^L^")
  var negemoticons = List(":(",":-(", ":'(", ":'-(",">:[", ":-c", ":c",":-<", ":<", ":-[", ":[", ":{",":'-(", ":'("," _( ",":'[","='(","' [","='[",":'-<", ":' <", ":'<", "=' <", "='<", "T_T", "T.T","(T_T)", "y_y","y.y","(Y_Y)",";-;", ";_;",";.;",":_:","o .__. o",".-.")


  def PreProcessing(sc:SparkContext): Tuple3[DataFrame,DataFrame,DataFrame] ={
    val path:String="./DATASET/Tweets/"
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    var tweets:DataFrame=null

    GetFileList(path).foreach{fileName=>
      if(tweets==null){
        tweets=ReadFile(path,fileName,fileName.split("_")(2),sc,sqlContext)
      }else{
        tweets=tweets.union(ReadFile(path,fileName,fileName.split("_")(2),sc,sqlContext))
      }
    }

    // SOSTITUZIONE SLANG WORDS ***********************************************
    import sqlContext.implicits._
    tweets=tweets
      .map(row=>(row.getString(0).replaceAll("disgust","disgust-hate"),
        UtilsPreProcessing.slang.getOrElse(row.getString(1),row.getString(1).toLowerCase).toLowerCase))
      .toDF()
    // **************************************************************************

    /*/ EMOJI COUNT *****************************************************
    var emojis = tweets
      .map(tweet => (tweet.getString(0),EmojiParser.extractEmojis(tweet.getString(1)).toString.split("")))
      .toDF("FEELING","EMOJIS")
    emojis=emojis
      .withColumn("SYMBOL", explode(emojis("EMOJIS")))
      .drop("EMOJIS")
      .filter(row=>row.getString(1)!="")
      .groupBy("FEELING","SYMBOL").count()
    // **************************************************************************/

    // SPLIT TWEETS IN LEMMAS ***************************************************
    var splittedTweets=tweets
      //.map(row=>(row.getString(0),EmojiParser.removeAllEmojis(row.getString(1)).split(" "))) //remove emojis/emoticons and split by space
      .map(row=>(row.getString(0), row.getString(1).split(" "))) //split by space
      .toDF("FEELING","TWEETS_WORDS_LIST") //tokenized tweets

    splittedTweets=splittedTweets
      .withColumn("LEMMA", explode(splittedTweets("TWEETS_WORDS_LIST")))
      .drop("TWEETS_WORDS_LIST")
    // **************************************************************************

    // EMOTICONS / EMOJIS COUNT *****************************************************
    var emoticons = splittedTweets.filter(row => (posemoticons contains row.getString(1)) || (negemoticons contains row.getString(1)) || EmojiManager.isEmoji(row.getString(1)))
    splittedTweets=splittedTweets.except(emoticons) //remove all emoticons from lemmas
    emoticons=emoticons
        .map(row => (row.getString(0), row.getString(1),EmojiParser.parseToAliases(row.getString(1)),EmojiParser.parseToHtmlHexadecimal(row.getString(1))))
      .toDF("FEELING","SYMBOL","ALIAS","HTML_HEX")
      .filter(row=> row.getString(1).length>0)
      .groupBy("FEELING","SYMBOL", "ALIAS","HTML_HEX").count()
    // **************************************************************************

    // PUNCTUATION, STOP WORDS AND FILTER ***************************************
    splittedTweets=splittedTweets.as[(String,String)]
      .map(t=>(t._1, CleanTweet(t._2:String)))
      .filter(row=> row._2!="" && !row._2.toLowerCase.contains("username") && !row._2.toLowerCase.contains("url") && row._2.length>2)//rimuove parole corte
      .toDF("FEELING","LEMMA")
    // **************************************************************************

    // HASHTAGS COUNT *****************************************************
    var hashtags = splittedTweets.filter(_.getString(1).contains("#"))
    splittedTweets=splittedTweets.except(hashtags) //remove all hashtags from lemmas
    hashtags=hashtags.groupBy("FEELING","LEMMA").count()
    // **************************************************************************

    (splittedTweets.groupBy("FEELING","LEMMA").count(), emoticons, hashtags)
  }

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  def ReadFile(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    println("read tweets ",path+filename)
    val tokenized = sc.wholeTextFiles(path+filename)
      .map(t=>(feelingName.toLowerCase,t._2.toLowerCase:String))
      .toDF("FEELING","TWEETS")
    return tokenized
  }

  def CleanTweet(tweet:String):String={
    var res=tweet
    res=StringUtils.replaceEach(res, this.punctuaction.toArray, Array.fill[String](this.punctuaction.length)(" "))//eliminare punteggiatura
    if(this.stopWords.indexOf(res) != -1) {res=""}//eliminare stop words
    res=res.replaceAll(" ","").replaceAll("\n","")
    res
  }

  def PrintToCSV(df: DataFrame): Unit ={
    println("start writing")
    df.coalesce(1)         // Writes to a single file
      .write
      .mode("overwrite")
      .format("csv")
      .save("./RESULTS/")
  }
}
