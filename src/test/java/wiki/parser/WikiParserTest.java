package wiki.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WikiParserTest {
  
  public static void main(String[] args) throws IOException {
    Path path = Paths.get("data/snippet.txt");
    String wtext = new String(Files.readAllBytes(path));
    String titleKey = "第一生命保険";

    WikiTextParser parser = new WikiTextParser(titleKey, wtext);
    System.out.println(parser.getTitleKana());
  }
  
}
