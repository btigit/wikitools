package mahout.vectorizer;

import java.io.IOException;
import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.util.Version;

public class JapaneseMahoutAnalyzer extends Analyzer {

  private final JapaneseAnalyzer japaneseAnalyzer = 
      new JapaneseAnalyzer(Version.LUCENE_36, null, JapaneseTokenizer.Mode.NORMAL, 
          JapaneseAnalyzer.getDefaultStopSet(), getDefaultStopTags());

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return japaneseAnalyzer.tokenStream(fieldName, reader);
  }
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return japaneseAnalyzer.reusableTokenStream(fieldName, reader);
  }
  
  public static Set<String> getDefaultStopTags(){
    Set<String> stopTags = JapaneseAnalyzer.getDefaultStopTags();
    stopTags.add("名詞-代名詞");
    stopTags.add("名詞-代名詞-一般");
    stopTags.add("名詞-代名詞-縮約");
    stopTags.add("名詞-数");
    return stopTags;
  }

  
}
