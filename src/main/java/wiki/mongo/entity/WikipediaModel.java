package wiki.mongo.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Index;
import com.google.code.morphia.annotations.Indexes;
import com.google.code.morphia.annotations.Transient;

@Entity(value="docs", noClassnameStored=true)
@Indexes({ @Index("docid"), @Index("title") })
public class WikipediaModel {
  
  @Id private ObjectId id;
  private long docid;
  private String title;
  private String titleAnnotation;
  private String titleKana;
  @Transient private String orgText;
  private String text;
  private String redirectText;
  private List<String> categories;
  private List<String> links;
  private Date lastModified;
  
  private boolean redirect;
  private boolean stub = false;
  private boolean disambiguation = false;
  private boolean empty = false;
  
  public enum DocumentType {
    DOCUMENT, STUB, DISAMBIGUATION, EMPTY
  }
  
  public Long getDocId() {
    return docid;
  }
  
  public void setDocId(Long docid) {
    this.docid = docid;
  }
  
  public String getTitle() {
    if (title == null)
      return "";
    return title;
  }
  
  public void setTitle(String title) {
    this.title = title;
  }
  
  public String getTitleAnnotation() {
    if (titleAnnotation == null)
      return "";
    return titleAnnotation;
  }
  
  public void setTitleAnnotation(String titleAnnotation) {
    if (titleAnnotation == null) {
      titleAnnotation = "";
    }
    this.titleAnnotation = titleAnnotation;
  }
  
  public String getTitleKana() {
    if (titleKana == null)
      return "";
    return titleKana;
  }

  public void setTitleKana(String titleKana) {
    if (titleKana == null) {
      titleKana = "";
    }
    this.titleKana = titleKana;
  }
  
  public String getOrgText() {
    return orgText;
  }
  
  public void setOrgText(String orgText) {
    this.orgText = orgText;
  }
  
  public String getText() {
    if (text == null)
      return "";
    return text;
  }
  
  public void setText(String text) {
    this.text = text;
  }
  
  public void setRedirectText(String redirectText) {
    this.redirectText = redirectText;
  }
  
  public String getRedirectText() {
    if (redirectText == null)
      return "";
    return redirectText;
  }
  
  public String getType() {
    if (empty) {
      return DocumentType.EMPTY.toString();
    } else if (disambiguation) {
      return DocumentType.DISAMBIGUATION.toString();
    } else if (stub) {
      return DocumentType.STUB.toString();
    } else {
      return DocumentType.DOCUMENT.toString();
    }
  }
  
  public Date getLastModified() {
    return lastModified;
  }
  
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }
  
  public List<String> getCategories() {
    if (categories == null)
      return new ArrayList<>();
    return categories;
  }
  
  public void setCategories(List<String> categories) {
    this.categories = categories;
  }
  
  public List<String> getLinks() {
    if (links == null)
      return new ArrayList<>();
    return links;
  }
  
  public void setLinks(List<String> links) {
    this.links = links;
  }
  
  public boolean isRedirect() {
    return redirect;
  }
  
  public void setRedirect(boolean redirect) {
    this.redirect = redirect;
  }
  
  public boolean isStub() {
    return stub;
  }
  
  public void setStub(boolean stub) {
    this.stub = stub;
  }
  
  public boolean isDisambiguation() {
    return disambiguation;
  }
  
  public void setDisambiguation(boolean disambiguation) {
    this.disambiguation = disambiguation;
  }
  
  public boolean isEmpty() {
    return empty;
  }
  
  public void setEmpty(boolean empty) {
    this.empty = empty;
  }
}