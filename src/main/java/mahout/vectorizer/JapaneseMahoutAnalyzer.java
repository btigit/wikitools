package mahout.vectorizer;


import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import lucene.hadoop.util.UnknownWordsTokenFilter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.ja.JapaneseBaseFormFilter;
import org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilter;
import org.apache.lucene.analysis.ja.JapanesePartOfSpeechStopFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

public class JapaneseMahoutAnalyzer extends StopwordAnalyzerBase {
  private final Mode mode;
  private final Set<String> stoptags;
  private final UserDictionary userDict;
  
  public JapaneseMahoutAnalyzer() {
    this(Version.LUCENE_40, DefaultSetHolder.USER_DICT, JapaneseTokenizer.DEFAULT_MODE, DefaultSetHolder.DEFAULT_STOP_SET, DefaultSetHolder.DEFAULT_STOP_TAGS);
  }
  
  public JapaneseMahoutAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.USER_DICT, JapaneseTokenizer.DEFAULT_MODE, DefaultSetHolder.DEFAULT_STOP_SET, DefaultSetHolder.DEFAULT_STOP_TAGS);
  }
  
  public JapaneseMahoutAnalyzer(Version matchVersion, UserDictionary userDict, Mode mode, CharArraySet stopwords, Set<String> stoptags) {
    super(matchVersion, stopwords);
    this.userDict = userDict;
    this.mode = mode;
    this.stoptags = stoptags;
  }
  
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  public static Set<String> getDefaultStopTags(){
    return DefaultSetHolder.DEFAULT_STOP_TAGS;
  }

  /**
   * Atomically loads DEFAULT_STOP_SET, DEFAULT_STOP_TAGS in a lazy fashion once the 
   * outer class accesses the static final set the first time.
   */
  private static class DefaultSetHolder {
    static CharArraySet DEFAULT_STOP_SET;
    static Set<String> DEFAULT_STOP_TAGS;
    static UserDictionary USER_DICT;

    static {
      try {
        DEFAULT_STOP_SET = loadStopwordSet(true, JapaneseMahoutAnalyzer.class, "stopwords_ja.txt", "#");  // ignore case
        final CharArraySet tagset = loadStopwordSet(false, JapaneseMahoutAnalyzer.class, "stoptags_ja.txt", "#");
        DEFAULT_STOP_TAGS = new HashSet<String>();
        for (Object element : tagset) {
          char chars[] = (char[]) element;
          DEFAULT_STOP_TAGS.add(new String(chars));
        }
      } catch (IOException ex) {
        // default set should always be present as it is part of the distribution (JAR)
        throw new RuntimeException("Unable to load default stopword or stoptag set");
      }
      try {
        USER_DICT = loadDictionary(JapaneseMahoutAnalyzer.class, "dictionary.csv", "#");
      } catch (IOException ex) {
        USER_DICT = null;
      }
    }
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer tokenizer = new JapaneseTokenizer(reader, userDict, true, mode);
    TokenStream stream = new JapaneseBaseFormFilter(tokenizer);
    stream = new UnknownWordsTokenFilter(stream, false);
    stream = new JapanesePartOfSpeechStopFilter(true, stream, stoptags);
    stream = new CJKWidthFilter(stream);
    stream = new StopFilter(matchVersion, stream, stopwords);
    stream = new JapaneseKatakanaStemFilter(stream);
    stream = new LowerCaseFilter(matchVersion, stream);
    stream = new PatternReplaceFilter(stream, Pattern.compile("([0-9]+)"), "", true);
    stream = new LengthFilter(true, stream, 3, 20);
    return new TokenStreamComponents(tokenizer, stream);
  }
  
  protected static UserDictionary loadDictionary(
      final Class<? extends Analyzer> aClass, final String resource,
      final String comment) throws IOException {
    Reader reader = null;
    try {
      reader = IOUtils.getDecodingReader(aClass.getResourceAsStream(resource), IOUtils.CHARSET_UTF_8);
      return new UserDictionary(reader);
    } finally {
      IOUtils.close(reader);
    }
    
  }
  
}
