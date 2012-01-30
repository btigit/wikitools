package tools;

import java.util.ArrayList;
import java.util.Date;

public class WikipediaModel {

	private long id;
    private String title;
    private String titleAnnotation;
    private String text;
    private String redirectText;
    private ArrayList<String> categories;
    private ArrayList<String> links;
    private Date lastModified;
	
    private boolean redirect;
	private boolean stub = false;
	private boolean disambiguation = false;
	private boolean empty = false;
	
	enum DocumentType { DOCUMENT, STUB, DISAMBIGUATION , EMPTY}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
    	if(title==null) return "";
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitleAnnotation() {
        return titleAnnotation;
    }

    public void setTitleAnnotation(String titleAnnotation) {
    	if(titleAnnotation==null){
    		titleAnnotation="";
    	}
        this.titleAnnotation = titleAnnotation;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

	public void setRedirectText(String redirectText) {
		this.redirectText=redirectText;
	}
	
	public String getRedirectText(){
		return redirectText;
	}

	public String getType(){
		if(empty){
			return DocumentType.EMPTY.toString();
		}else if(disambiguation){
			return DocumentType.DISAMBIGUATION.toString();
		}else if(stub){
			return DocumentType.STUB.toString();
		}else{
			return DocumentType.DOCUMENT.toString();
		}
	}
	
    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }
    
    public ArrayList<String> getCategories() {
		return categories;
	}

	public void setCategories(ArrayList<String> categories) {
		this.categories = categories;
	}
	
	public ArrayList<String> getLinks() {
		return links;
	}

	public void setLinks(ArrayList<String> links) {
		this.links=links;
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