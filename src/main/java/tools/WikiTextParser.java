package tools;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;

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
	private ArrayList<String> pageCats = null;
	private ArrayList<String> pageLinks = null;
	private boolean redirect = false;
	private String redirectString = null;
	private static Pattern redirectPattern = Pattern
			.compile("#(REDIRECT|転送)\\s+\\[\\[(.*?)\\]\\]");
	private boolean stub = false;
	private boolean disambiguation = false;
	private static Pattern stubPattern = Pattern.compile("\\-stub\\}\\}");
	private static Pattern disambCatPattern = Pattern
			.compile("\\{\\{disambig\\}\\}");
	private InfoBox infoBox = null;

    /** XMLで使われてる日付形式 */
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    
	public WikiTextParser(String wtext) {
		wikiText = wtext;
		Matcher matcher = redirectPattern.matcher(wikiText);
		if (matcher.find()) {
			redirect = true;
			redirectString = matcher.group(2);
		}
		matcher = stubPattern.matcher(wikiText);
		stub = matcher.find();
		matcher = disambCatPattern.matcher(wikiText);
		disambiguation = matcher.find();
	}
	
    /** page element内の解析 */
    public static WikipediaModel pageParse(XMLEventReader reader) throws Exception {
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
                model.setId(Long.parseLong(getText(reader, "id")));
        }
        return model;
    }

    /** revision element内の解析 */
    public static void revisionParse(XMLEventReader reader, WikipediaModel model) throws Exception {
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (isEndElem(event, "revision")){
                break;
        	}else if (isStartElem(event, "text")){
            	WikiTextParser parser = new WikiTextParser(getText(reader, "text"));
            	model.setText(parser.getPlainText());
            	model.setRedirect(parser.isRedirect());
            	model.setRedirectText(parser.getRedirectText());
            	model.setStub(parser.isStub());
            	model.setDisambiguation(parser.isDisambiguationPage());
            	model.setCategories(parser.getCategories());
            	model.setLinks(parser.getLinks());
            }else if (isStartElem(event, "timestamp")){
                model.setLastModified(sdf.parse(getText(reader, "timestamp")));
        	}
        }
    }

    /** 指定のend tagを発見するまで、CHARACTERSを取得 */
    public static String getText(XMLEventReader reader, String name) throws Exception {
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

	public boolean isStub() {
		return stub;
	}

	public String getRedirectText() {
		return redirectString;
	}

	public String getText() {
		return wikiText;
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

		Pattern catPattern = Pattern.compile("\\[\\[(.*?)\\]\\]",
				Pattern.MULTILINE);
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
			if(sentence.trim().endsWith("。")){
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
