package wiki.parser;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;

import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;

import wiki.mongo.entity.WikipediaModel;
import edu.jhu.nlp.wikipedia.WikiPage;

/**
 * For internal use only -- Used by the {@link WikiPage} class. Can also be used
 * as a stand alone class to parse wiki formatted text.
 * 
 * @author Delip Rao
 * 
 */
public class WikiTextParser {
  
  private String wikiText = null;
  private String titleKana = null;
  private ArrayList<String> pageCats = null;
  private ArrayList<String> pageLinks = null;
  private boolean redirect = false;
  private boolean softRedirect = false;
  private String redirectString = null;
  private static Pattern redirectPattern = Pattern
      .compile("#([Rr][Ee][Dd][Ii][Rr][Ee][Cc][Tt]|転送):*\\s*\\[\\[(.*?)\\]\\]");
  private String softRedirectString = null;
  private static Pattern softredirectPattern = Pattern
      .compile("\\{\\{(softredirect)|(.*?)\\}\\}");
  private boolean stub = false;
  private boolean disambiguation = false;
  private static Pattern stubPattern = Pattern.compile("\\-stub\\}\\}");
  private static Pattern disambCatPattern = Pattern
      .compile("\\{\\{(disambig|.*[Aa]imai.*|.*曖昧.*)\\}\\}");
  private InfoBox infoBox = null;
  private static Pattern kanaTitlePattern = Pattern.compile("'''([^']+?)'''");
  private static Tokenizer tokenizer = Tokenizer.builder().build();
  
  /** XMLで使われてる日付形式 */
  private static final SimpleDateFormat sdf = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'");
  
  public WikiTextParser(String title, String wtext) {
    wikiText = wtext;
    Matcher matcher = redirectPattern.matcher(wikiText);
    if (matcher.find()) {
      redirect = true;
      redirectString = matcher.group(2);
    }
    matcher = softredirectPattern.matcher(wikiText);
    if (matcher.find()) {
      softRedirect = true;
      softRedirectString = matcher.group(2);
    }
    matcher = stubPattern.matcher(wikiText);
    stub = matcher.find();
    matcher = disambCatPattern.matcher(wikiText);
    disambiguation = matcher.find();
    
    if(isKana(title)){
      titleKana = transformKana(title);
    }else{
      //読みがなを記事から抽出
      matcher = kanaTitlePattern.matcher(wikiText);
      //タイトルにスペースが含まれたものが文中にある場合を考慮
      String titleKey = title;
      if(matcher.find()){
        String tempTitle = matcher.group(1);
        if(!tempTitle.equals(title) && (
            tempTitle.replaceAll("[\\s　]", "").equals(title)
            || tempTitle.replaceAll("株式会社", "").equals(title))){
          titleKey = tempTitle;
        }
      }
      
      //読みがな抽出
      try {
        matcher = Pattern.compile("\n("+titleKey+"|'''"+titleKey+"'''|\\[\\["+titleKey+"\\]\\]|『"+titleKey+"』|『'''"+titleKey+"'''』)[\\s]*[\\(（]([^{]+?)[\\)）]").matcher(wikiText);
        if(matcher.find()){
          for (String token : matcher.group(2).split("[\\s　]*[,、，。\\[]|英称| - ")) {
            String titleKana = transformKana(token.trim()).replaceAll("'''", "").replaceAll("　", " ");
            //不正に抽出される場合を除く
            if(titleKana.indexOf("&mdash;") < 0 
                && titleKana.indexOf("<ref>") < 0 
                && isValidateKana(titleKana)){
              //TODO なかざわ のぶたかを2トークンに
              this.titleKana = transformKana(titleKana);
              break;
            }
          }
        }
      } catch (Exception e) {
        //ignore
      }
    }
    

    if(titleKana == null || titleKana.length() == 0 || !isValidateKana(titleKana)
        || (!containAlphabets(title) && containAlphabets(titleKana))){
      //抽出できなかった場合にはKuromoji利用
      StringBuffer buf = new StringBuffer();
      for (Token token : tokenizer.tokenize(title)) {
        if(token.isKnown()){
          buf.append(token.getReading());
        }else if(token.getSurfaceForm().matches("^[\\w\\u3040-\\u309F\\u30A0-\\u30FF]+$")){
          buf.append(token.getSurfaceForm());
        }
      }
      titleKana = transformKana(buf.toString());
    }
    
    if(titleKana!=null && !isValidateKana(titleKana)){
      System.out.println("invalid kana "+title+":"+titleKana);
    }
  }
  
//  private boolean isPunctuation(String str){
//    return str.matches("^[\\p{Punct}]*$");
//  }
  
//  private boolean isOmittable(String str){
//    return 
//             str.matches("^(紀元前)*\\d+(年|月|日|年代|世紀)$") 
//          || str.matches("^\\d+年の.+$")
//          || str.matches("^[\\p{Punct}\\w\\s]*$") 
//          || str.matches("^.*一覧$")
//          || str.matches("^.*一覧\\s[\\d]\\-[\\d]$")
//          || str.matches("^.*の一覧-.+$")
//          || str.matches("^.*/履歴$")
//          || str.matches("^.*/log[\\w]*$")
//          || str.matches("^.*/history[\\w]*$")
//          || str.matches("^.*の属種$")
//          || str.matches("^.*用語\\s[\\u3040-\\u309F]$")
//          || str.matches("^.*大学$") //TODO 大学はふりがなから参照可能
//          ;
//  }
  
  private boolean containAlphabets(String str){
    return str.matches(".*[\\w\\-].*");
  }
  
  private boolean isKana(String str){
    return str.matches("^[\\s\\w\\u3040-\\u309F\\u30A0-\\u30FF\\p{Punct}〜・]*$");
  }
  
  private boolean isValidateKana(String str){
    return str.matches("^[\\s\\w\\u3040-\\u309F\\u30A0-\\u30FF\\p{Punct}〃！”＃＄％＆’〔〕（）＝～｜‖‘｛＋＊｝＜＞？￣＿－＾￥＠「；：」、。・¨‥…々◇◆☆★◎〇○●×△▲□■〜『』←↑↓→—［］＼／〆“”〈〉【】:《》]*$");
  }
  
//  private String replaceAll(String s) {
//    return s.replaceAll("[\\p{Punct}！”＃＄％＆’（）＝～｜‘｛＋＊｝＜＞？＿－＾￥＠「；：」、。・…々☆★◎○●×△▲□〜『』↑↓→‐［］／〆“”〈〉【】:《》]", "");
//  }
  
  private String transformKana(String s) {
    StringBuffer sb = new StringBuffer(s);
    for (int i = 0; i < sb.length(); i++) {
      char c = sb.charAt(i);
      if (c >= 'ぁ' && c <= 'ん') {
        sb.setCharAt(i, (char)(c - 'ぁ' + 'ァ'));
      }
    }
    return sb.toString().replaceAll("#.*$", "").replaceAll("´", "").replaceAll("‐", "ー").replaceAll("〜", "ー").replaceAll("~", "ー");
  }
  
  /** page element内の解析 */
  public static WikipediaModel pageParse(XMLEventReader reader)
      throws Exception {
    WikipediaModel model = new WikipediaModel();
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (isEndElem(event, "page"))
        break;
      // revision elementの解析は、revisonParseにて行う
      else if (isStartElem(event, "revision"))
        revisionParse(reader, model);
      // title
      else if (isStartElem(event, "title")) {
        String title = getText(reader, "title");
        // タイトルにコロンが含まれる場合は管理用記事なのでスキップする
        if (title.indexOf(':') != -1)
          return null;
        // (曖昧さ回避)や(音楽)などの注釈文字を外す
        int posStart = title.indexOf(" (");
        int posEnd = title.indexOf(')', posStart);
        if (posStart != -1 && posEnd != -1) {
          model.setTitle(title.substring(0, posStart));
          model.setTitleAnnotation(title.substring(posStart + 2, posEnd));
        } else {
          model.setTitle(title);
        }
      } else if (isStartElem(event, "id"))
        model.setDocId(Long.parseLong(getText(reader, "id")));
    }
    return model;
  }
  
  /** revision element内の解析 */
  public static void revisionParse(XMLEventReader reader, WikipediaModel model)
      throws Exception {
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (isEndElem(event, "revision")) {
        break;
      } else if (isStartElem(event, "text")) {
        model.setOrgText(getText(reader, "text"));
      } else if (isStartElem(event, "timestamp")) {
        model.setLastModified(sdf.parse(getText(reader, "timestamp")));
      }
    }
  }
  
  /** 指定のend tagを発見するまで、CHARACTERSを取得 */
  public static String getText(XMLEventReader reader, String name)
      throws Exception {
    StringBuilder builder = new StringBuilder();
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (isEndElem(event, name))
        break;
      else if (event.getEventType() == XMLStreamConstants.CHARACTERS) {
        String data = event.asCharacters().getData().trim();
        if (data.length() > 0)
          builder.append(data);
      }
    }
    return builder.toString();
  }
  
  /** 指定名のStart Elementか判定する */
  public static boolean isStartElem(XMLEvent event, String name) {
    return event.getEventType() == XMLStreamConstants.START_ELEMENT
        && name.equals(event.asStartElement().getName().getLocalPart());
  }
  
  /** 指定名のEnd Elementか判定する */
  public static boolean isEndElem(XMLEvent event, String name) {
    return event.getEventType() == XMLStreamConstants.END_ELEMENT
        && name.equals(event.asEndElement().getName().getLocalPart());
  }
  
  public boolean isRedirect() {
    return redirect;
  }
  
  public boolean isSoftRedirect() {
    return softRedirect;
  }
  
  public boolean isStub() {
    return stub;
  }
  
  public String getRedirectText() {
    return redirectString;
  }
  
  public String getSoftRedirectText() {
    return softRedirectString;
  }
  
  public String getText() {
    return wikiText;
  }
  
  public String getTitleKana() {
    return titleKana;
  }
  
  public ArrayList<String> getCategories() {
    if (pageCats == null)
      parseCategories();
    return pageCats;
  }
  
  public ArrayList<String> getLinks() {
    if (pageLinks == null)
      parseLinks();
    return pageLinks;
  }
  
  private void parseCategories() {
    pageCats = new ArrayList<String>();
    Pattern catPattern = Pattern.compile("\\[\\[Category:(.*?)\\]\\]",
        Pattern.MULTILINE);
    Matcher matcher = catPattern.matcher(wikiText);
    while (matcher.find()) {
      String[] temp = matcher.group(1).split("\\|");
      pageCats.add(temp[0]);
    }
  }
  
  private void parseLinks() {
    pageLinks = new ArrayList<String>();
    
    Pattern catPattern = Pattern
        .compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
    Matcher matcher = catPattern.matcher(wikiText);
    while (matcher.find()) {
      String[] temp = matcher.group(1).split("\\|");
      if (temp == null || temp.length == 0)
        continue;
      String link = temp[0];
      if (link.contains(":") == false) {
        pageLinks.add(link);
      }
    }
  }
  
  public String getPlainText() {
    String text = wikiText;
    text = text.replaceAll("&lt;ref&gt;", "（");
    text = text.replaceAll("&lt;/ref&gt", "）");
    text = text.replaceAll("&gt;", ">");
    text = text.replaceAll("&lt;", "<");
    text = text.replaceAll("<ref>.*?</ref>", " ");
    text = text.replaceAll("</?.*?>", " ");
    text = text.replaceAll("\\{\\{.*?\\}\\}", " ");
    text = text.replaceAll("\\[\\[.*?:.*?\\]\\]", " ");
    text = text.replaceAll("\\[\\[(.*?)\\]\\]", "$1");
    text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
    text = text.replaceAll("\\[.*?\\]", " ");
    text = text.replaceAll("\\'+", "");
    
    String[] sentences = text.split("\n");
    StringBuffer buf = new StringBuffer();
    for (String sentence : sentences) {
      if (sentence.trim().endsWith("。")) {
        buf.append(sentence.trim());
        buf.append("\n");
      }
    }
    return buf.toString();
  }
  
  public InfoBox getInfoBox() {
    // parseInfoBox is expensive. Doing it only once like other parse*
    // methods
    if (infoBox == null)
      infoBox = parseInfoBox();
    return infoBox;
  }
  
  private InfoBox parseInfoBox() {
    String INFOBOX_CONST_STR = "{{Infobox";
    int startPos = wikiText.indexOf(INFOBOX_CONST_STR);
    if (startPos < 0)
      return null;
    int bracketCount = 2;
    int endPos = startPos + INFOBOX_CONST_STR.length();
    for (; endPos < wikiText.length(); endPos++) {
      switch (wikiText.charAt(endPos)) {
        case '}':
          bracketCount--;
          break;
        case '{':
          bracketCount++;
          break;
        default:
      }
      if (bracketCount == 0)
        break;
    }
    if (endPos + 1 >= wikiText.length())
      return null;
    // This happens due to malformed Infoboxes in wiki text. See Issue #10
    // Giving up parsing is the easier thing to do.
    String infoBoxText = wikiText.substring(startPos, endPos + 1);
    infoBoxText = stripCite(infoBoxText); // strip clumsy {{cite}} tags
    // strip any html formatting
    infoBoxText = infoBoxText.replaceAll("&gt;", ">");
    infoBoxText = infoBoxText.replaceAll("&lt;", "<");
    infoBoxText = infoBoxText.replaceAll("<ref.*?>.*?</ref>", " ");
    infoBoxText = infoBoxText.replaceAll("</?.*?>", " ");
    return new InfoBox(infoBoxText);
  }
  
  private String stripCite(String text) {
    String CITE_CONST_STR = "{{cite";
    int startPos = text.indexOf(CITE_CONST_STR);
    if (startPos < 0)
      return text;
    int bracketCount = 2;
    int endPos = startPos + CITE_CONST_STR.length();
    for (; endPos < text.length(); endPos++) {
      switch (text.charAt(endPos)) {
        case '}':
          bracketCount--;
          break;
        case '{':
          bracketCount++;
          break;
        default:
      }
      if (bracketCount == 0)
        break;
    }
    text = text.substring(0, startPos - 1) + text.substring(endPos);
    return stripCite(text);
  }
  
  public boolean isDisambiguationPage() {
    return disambiguation;
  }
  
  public String getTranslatedTitle(String languageCode) {
    Pattern pattern = Pattern.compile("^\\[\\[" + languageCode
        + ":(.*?)\\]\\]$", Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(wikiText);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }
  
}
