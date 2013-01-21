package wiki.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.io.WikiWritable;
import wiki.mongo.entity.WikipediaModel;
import wiki.parser.WikiTextParser;

public class WikipediaDumper {
  
  public static class Map extends MapReduceBase implements Mapper<Text, Text, LongWritable, WikiWritable> {

    private static final Logger log = LoggerFactory.getLogger(Map.class);
    private static final String START_DOC = "<text xml:space=\"preserve\">";
    private static final String END_DOC = "</text>";
    private static final Pattern ID = Pattern.compile("<id>(.*)<\\/id>");
    private static final Pattern TITLE = Pattern.compile("<title>(.*)<\\/title>");
    
    private Set<String> inputCategories;
    private boolean exactMatchOnly;
    private boolean all;
    private boolean redirect;
    private boolean stub;
    private boolean disambiguation;

    @Override
    public void map(Text key, Text value,
        OutputCollector<LongWritable, WikiWritable> output, Reporter reporter) throws IOException {
      String content = key.toString();
      Long docId;
      String document;
      String title;
      try {
        docId = getDocId(content);
        document = StringEscapeUtils.unescapeHtml(getDocument(content));
        title = StringEscapeUtils.unescapeHtml(getTitle(content));
      } catch (RuntimeException e) {
        reporter.getCounter("Wikipedia", "Parse errors").increment(1);
        return;
      }

      if (title.indexOf(':') != -1){
        reporter.getCounter("Wikipedia", "Admin pages").increment(1);
        return;
      }
      
      WikipediaModel model = new WikipediaModel();
      model.setDocId(docId);
      // (曖昧さ回避)や(音楽)などの注釈文字を外す
      int posStart = title.indexOf(" (");
      int posEnd = title.indexOf(')', posStart);
      if (posStart != -1 && posEnd != -1) {
        model.setTitle(title.substring(0, posStart));
        model.setTitleAnnotation(title.substring(posStart + 2, posEnd));
      } else {
        model.setTitle(title);
      }
      model.setOrgText(document);
      
      WikiTextParser parser = new WikiTextParser(model.getTitle(), model.getOrgText());
      model.setText(parser.getPlainText());
      model.setTitleKana(parser.getTitleKana());
      model.setRedirect(parser.isRedirect());
      model.setRedirectText(parser.getRedirectText());
      model.setStub(parser.isStub());
      model.setDisambiguation(parser.isDisambiguationPage());
      model.setCategories(parser.getCategories());
      model.setLinks(parser.getLinks());
      
      if (!redirect && model.isRedirect()){
        reporter.getCounter("Wikipedia", "Redirect pages").increment(1);
        return;
      }
      
      if (!stub && model.isStub()){
        reporter.getCounter("Wikipedia", "Stub pages").increment(1);
        return;
      }
      
      if (!disambiguation && model.isDisambiguation()){
        reporter.getCounter("Wikipedia", "Disambiguation pages").increment(1);
        return;
      }
      
      if (!all) {
        if (!findMatchingCategory(parser.getCategories())) {
          return;
        }
      }
      reporter.getCounter("Wikipedia", "Normal pages").increment(1);
      output.collect(new LongWritable(model.getDocId()), new WikiWritable(model));
    }
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      if (inputCategories == null) {
        Set<String> newCategories = new HashSet<String>();

        DefaultStringifier<Set<String>> setStringifier =
            new DefaultStringifier<Set<String>>(job, GenericsUtil.getClass(newCategories));

        String categoriesStr;
        try {
          categoriesStr = job.get("wikipedia.categories", setStringifier.toString(newCategories));
          inputCategories = setStringifier.fromString(categoriesStr);
        } catch (IOException e) {
        }
      }
      exactMatchOnly = job.getBoolean("exact.match.only", false);
      all = job.getBoolean("all.files", true);
      redirect = job.getBoolean("redirect.pages", false);
      stub = job.getBoolean("stub.pages", false);
      disambiguation = job.getBoolean("disambiguation.pages", false);
      log.info("Configure: Input Categories size: {} All: {} Exact Match: {} Redirect: {} Stub: {} Disambiguation: {}",
               new Object[] {inputCategories.size(), all, exactMatchOnly, redirect, stub, disambiguation});
    }
    
    private static Long getDocId(CharSequence xml) {
      Matcher m = ID.matcher(xml);
      return m.find() ? Long.parseLong(m.group(1)) : -1;
    }

    private static String getDocument(String xml) {
      int start = xml.indexOf(START_DOC) + START_DOC.length();
      int end = xml.indexOf(END_DOC, start);
      return xml.substring(start, end);
    }

    private static String getTitle(CharSequence xml) {
      Matcher m = TITLE.matcher(xml);
      return m.find() ? m.group(1) : "";
    }

    private boolean findMatchingCategory(ArrayList<String> arrayList) {
      for (String category : arrayList) {
        if (exactMatchOnly && inputCategories.contains(category)) {
          return true;
        }
        if (!exactMatchOnly) {
          for (String inputCategory : inputCategories) {
            if (category.contains(inputCategory)) { // we have an inexact match
              return true;
            }
          }
        }
      }
      return false;
    }
  }
}
