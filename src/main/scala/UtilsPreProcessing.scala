import org.apache.spark.SparkContext

object UtilsPreProcessing {

  var posemoticons = List("B-)",":)", ":-)", ":')", ":'-)", ":D", ":-D",":\'-)",":')",":o)",":]",":3",":c)",":>","=]","8)","=)",":}",":^)","8-D","8D","x-D","xD","X-D","XD","=-D","=D","=-3","=3","B^D",":-))",":*",":^*","( \'}{\' )","^^","(^_^)","^-^","^.^","^3\\^","\\^L\\^");
  var negemoticons = List(":(",":-(", ":'(", ":'-(",">:[", ":-c", ":c",":-<", ":<", ":-[", ":[", ":{",":\'-(", ":\'("," _( ",":\'[","='(","' [","='[",":'-<", ":' <", ":'<", "=' <", "='<", "T_T", "T.T","(T_T)", "y_y","y.y","(Y_Y)",";-;", ";_;",";.;",":_:","o .__. o",".-.");
  var punctuaction = List(">", "<", "=", "+", "_", "&", ")", "(", "/", "\\", ":", ";", ".", "!", "?", ",")
  var slang = Map[String, String]("afaik"->"as far as i know","afk"->"away from keyboard","asap"->"as soon as possible","atk"->"at the keyboard","atm"->"at the moment","a3"->"anytime, anywhere, anyplace","bak"->"back at keyboard","bbl"->"be back later","bbs"->"be back soon","bfn"->"bye for now","b4n"->"bye for now","brb"->"be right back","brt"->"be right there","btw"->"by the way","b4n"->"bye for now","cu"->"see you","cul8r"->"see you later","cya"->"see you","faq"->"frequently asked questions","fc"->"fingers crossed","fwiw"->"for what it\"s worth","fyi"->"for your information","gal"->"get a life","gg"->"good game","gmta"->"great minds think alike","gr8"->"great!","g9"->"genius","ic"->"i see","icq"->"i seek you","ilu"->"ilu:i love you","imho"->"in my honest opinion","imo"->"in my opinion","iow"->"in other words","irl"->"in real life","kiss"->"keep it simple, stupid","ldr"->"long distance relationship","lmao"->"laugh my a.. off","lol"->"laughing out loud","ltns"->"long time no see","l8r"->"later","mte"->"my thoughts exactly","m8"->"mate","nrn"->"no reply necessary","oic"->"oh i see","pita"->"pain in the a..","prt"->"party","prw"->"parents are watching","qpsa?"->"que pasa?","rofl"->"rolling on the floor laughing","roflol"->"rolling on the floor laughing out loud","rotflmao"->"rolling on the floor laughing my a.. off","sk8"->"skate","stats"->"your sex and age","asl"->"age, sex, location","thx"->"thank you","ttfn"->"ta-ta for now!","ttyl"->"talk to you later","u"->"you","u2"->"you too","u4e"->"yours for ever","wb"->"welcome back","wtf"->"what the f...","wtg"->"way to go!","wuf"->"where are you from?","w8"->"wait...","7k"->"sick:-d laugher")
  var stopWords = List("a","able","about","above","abst","accordance","according","accordingly","across","act","actually","added","adj","affected","affecting","affects","after","afterwards","again","against","ah","all","almost","alone","along","already","also","although","always","am","among","amongst","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","apparently","approximately","are","aren","arent","arise","around","as","aside","ask","asking","at","auth","available","away","awfully","b","back","be","became","because","become","becomes","becoming","been","before","beforehand","begin","beginning","beginnings","begins","behind","being","believe","below","beside","besides","between","beyond","biol","both","brief","briefly","but","by","c","ca","came","can","cannot","can't","cause","causes","certain","certainly","co","com","come","comes","contain","containing","contains","could","couldnt","d","date","did","didn't","different","do","does","doesn't","doing","done","don't","down","downwards","due","during","e","each","ed","edu","effect","eg","eight","eighty","either","else","elsewhere","end","ending","enough","especially","et","et-al","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","except","f","far","few","ff","fifth","first","five","fix","followed","following","follows","for","former","formerly","forth","found","four","from","further","furthermore","g","gave","get","gets","getting","give","given","gives","giving","go","goes","gone","got","gotten","h","had","happens","hardly","has","hasn't","have","haven't","having","he","hed","hence","her","here","hereafter","hereby","herein","heres","hereupon","hers","herself","hes","hi","hid","him","himself","his","hither","home","how","howbeit","however","hundred","i","id","ie","if","i'll","im","immediate","immediately","importance","important","in","inc","indeed","index","information","instead","into","invention","inward","is","isn't","it","itd","it'll","its","itself","i've","j","just","k","keep	","keeps","kept","kg","km","know","known","knows","l","largely","last","lately","later","latter","latterly","least","less","lest","let","lets","like","liked","likely","line","little","'ll","look","looking","looks","ltd","m","made","mainly","make","makes","many","may","maybe","me","mean","means","meantime","meanwhile","merely","mg","might","million","miss","ml","more","moreover","most","mostly","mr","mrs","much","mug","must","my","myself","n","na","name","namely","nay","nd","near","nearly","necessarily","necessary","need","needs","neither","never","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","now","nowhere","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","omitted","on","once","one","ones","only","onto","or","ord","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","owing","own","p","page","pages","part","particular","particularly","past","per","perhaps","placed","please","plus","poorly","possible","possibly","potentially","pp","predominantly","present","previously","primarily","probably","promptly","proud","provides","put","q","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","respectively","resulted","resulting","results","right","run","s","said","same","saw","say","saying","says","sec","section","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sent","seven","several","shall","she","shed","she'll","shes","should","shouldn't","show","showed","shown","showns","shows","significant","significantly","similar","similarly","since","six","slightly","so","some","somebody","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","still","stop","strongly","sub","substantially","successfully","such","sufficiently","suggest","sup","sure	","t","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that'll","thats","that've","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","thered","therefore","therein","there'll","thereof","therere","theres","thereto","thereupon","there've","these","they","theyd","they'll","theyre","they've","think","this","those","thou","though","thoughh","thousand","throug","through","throughout","thru","thus","til","tip","to","together","too","took","toward","towards","tried","tries","truly","try","trying","ts","twice","two","u","un","under","unfortunately","unless","unlike","unlikely","until","unto","up","upon","ups","us","use","used","useful","usefully","usefulness","uses","using","usually","v","value","various","'ve","very","via","viz","vol","vols","vs","w","want","wants","was","wasnt","way","we","wed","welcome","we'll","went","were","werent","we've","what","whatever","what'll","whats","when","whence","whenever","where","whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether","which","while","whim","whither","who","whod","whoever","whole","who'll","whom","whomever","whos","whose","why","widely","willing","wish","with","within","without","wont","words","world","would","wouldnt","www","x","y","yes","yet","you","youd","you'll","your","youre","yours","yourself","yourselves","you've","z","zero")
  var negationWords = List("not","rather","hardly","couldn\'t","wasn\'t","didn\'t","wouldn\'t","shouldn\'t","weren\'t","don\'t","doesn\'t","haven\'t","hasn\'t","won\'t","wont","hadn\'t")
  var emojiPos = List[String]()
  var emojiNeg = List[String]()
  var othersEmoji = List[String]()
  var additionalEmoji = List[String]()

  def PreProcessing(sc:SparkContext): Unit = {

    // Create our Emojis from Textfile to UNICODE HEXADECIMAL
    var tmpFileLines = Array[String]()
    sc.textFile("./DATASET/TweetsUtils/Emoji.txt")
      .foreach { emoticonsList =>
        tmpFileLines = emoticonsList.toString.split(" = \\[")
        if(tmpFileLines(0) == "EmojiPos"){
          emojiPos = SplitEmoji(tmpFileLines(1),"u'\\\\U000")
        } else if(tmpFileLines(0) == "EmojiNeg"){
          emojiNeg = SplitEmoji(tmpFileLines(1),"u'\\\\U000")
        } else if(tmpFileLines(0) == "OthersEmoji"){
          othersEmoji = SplitEmoji(tmpFileLines(1),"u'\\\\U000")
        } else if(tmpFileLines(0) == "AdditionalEmoji"){
          additionalEmoji = SplitEmoji(tmpFileLines(1),"u'\\\\U[+]")
        }
      }

    // Convert our Emojis to Tweet's Emojis.
    // var castToCheckTweetEmojiVariable = new String(Character.toChars(0x231B))

    /*println(posemoticons)
    println(negemoticons)
    println(punctuaction)
    println(slang)
    println(stopWords)
    println(negationWords)
    println(emojiPos)
    println(emojiNeg)
    println(othersEmoji)
    println(additionalEmoji)*/

  }

  def SplitEmoji(emojiList:String, regex:String): List[String] ={
    var dataStructure = List[String]()
    emojiList.dropRight(1).split(",").foreach { emoji =>
      dataStructure = emoji.replaceAll(regex,"0x").replaceAll("\'","")::dataStructure
    }
    return dataStructure
  }

}
