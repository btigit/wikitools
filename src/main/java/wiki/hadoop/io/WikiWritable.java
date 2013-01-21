package wiki.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import wiki.mongo.entity.WikipediaModel;

public class WikiWritable implements Writable{

  private LongWritable docid; 
  private Text title; 
  private Text titleAnnotation; 
  private Text titleKana; 
  private Text text; 
  private Text redirectText; 
  private TextArrayWritable categories; 
  private TextArrayWritable links; 
  private BooleanWritable redirect;
  private BooleanWritable stub;
  private BooleanWritable disambiguation;
  
  public WikiWritable(){
    this.docid = new LongWritable();
    this.title = new Text();
    this.titleAnnotation = new Text();
    this.titleKana = new Text();
    this.text = new Text();
    this.redirectText = new Text();
    this.categories = new TextArrayWritable();
    this.links = new TextArrayWritable();
    this.redirect = new BooleanWritable();
    this.stub = new BooleanWritable();
    this.disambiguation = new BooleanWritable();
  }
  
  public WikiWritable(WikipediaModel model){
    this.set(model);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    docid.write(out);
    title.write(out);
    titleAnnotation.write(out);
    titleKana.write(out);
    text.write(out);
    redirectText.write(out);
    categories.write(out);
    links.write(out);
    redirect.write(out);
    stub.write(out);
    disambiguation.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    docid.readFields(in);
    title.readFields(in);
    titleAnnotation.readFields(in);
    titleKana.readFields(in);
    text.readFields(in);
    redirectText.readFields(in);
    categories.readFields(in);
    links.readFields(in);
    redirect.readFields(in);
    stub.readFields(in);
    disambiguation.readFields(in);
  }
  
  public void set(WikipediaModel model) {
    this.docid = new LongWritable(model.getDocId());
    this.title = new Text(model.getTitle());
    this.titleAnnotation = new Text(model.getTitleAnnotation());
    this.titleKana = new Text(model.getTitleKana());
    this.text = new Text(model.getText());
    TextArrayWritable categories = new TextArrayWritable();
    Text[] categoriesArray = new Text[model.getCategories().size()];
    for (int i = 0; i < model.getCategories().size(); i++) {
      categoriesArray[i] = new Text(model.getCategories().get(i));
    }
    categories.set(categoriesArray);
    this.categories = categories;
    TextArrayWritable links = new TextArrayWritable();
    Text[] linksArray = new Text[model.getLinks().size()];
    for (int i = 0; i < model.getLinks().size(); i++) {
      linksArray[i] = new Text(model.getLinks().get(i));
    }
    links.set(linksArray);
    this.links = links;
    this.redirectText = new Text(model.getRedirectText());
    this.redirect = new BooleanWritable(model.isRedirect());
    this.stub = new BooleanWritable(model.isStub());
    this.disambiguation = new BooleanWritable(model.isDisambiguation());
  }
  
  public Long getDocid() {
    return docid.get();
  }

  public Text getTitle() {
    return title;
  }

  public Text getTitleAnnotation() {
    return titleAnnotation;
  }

  public Text getTitleKana() {
    return titleKana;
  }

  public Text getText() {
    return text;
  }

  public Text getRedirectText() {
    return redirectText;
  }

  public TextArrayWritable getCategories() {
    return categories;
  }
  
  public TextArrayWritable getLinks() {
    return links;
  }
  
  public BooleanWritable getRedirect() {
    return redirect;
  }

  public BooleanWritable getStub() {
    return stub;
  }

  public BooleanWritable getDisambiguation() {
    return disambiguation;
  }
  
  public long length(){
    long len = 0L;
    return len;
  }
  
}