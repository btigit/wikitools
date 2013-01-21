package mahout.vectorizer;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class JapaneseMahoutAnalyzerTest {
  
  public static void main(String[] args) throws IOException {
    try (JapaneseMahoutAnalyzer analyzer = new JapaneseMahoutAnalyzer()){
      TokenStream stream = analyzer.tokenStream("hoge", new StringReader("今日はブレインズテクノロジーがとっても熱いです"));
      CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        if (termAtt.length() > 0) {
          System.out.println(new String(termAtt.buffer(), 0, termAtt.length()));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
}
