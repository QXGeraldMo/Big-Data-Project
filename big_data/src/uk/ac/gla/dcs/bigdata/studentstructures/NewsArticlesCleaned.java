package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


/**
 * Qixiang Mo
 * Jingyi Mao 
 * Ziyang Lin 
 */

public class NewsArticlesCleaned implements Serializable {
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id; // unique article identifier
	List<String> title; // article title
	List<String> paragraph; // the contents of the paragraph 
	Long doc_length; //article's length
	HashMap<String, Long> wordMap = new HashMap<String, Long>();//this map is to record word-frequency pair.
	NewsArticle originalArticle;//get original object 

	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph, Long doc_length, NewsArticle originalArticle) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.doc_length = doc_length;
		WordFrequency();
		this.originalArticle = originalArticle;
	}


	public NewsArticlesCleaned() {
		// TODO Auto-generated constructor stub
	}


	public Long getDoc_length() {
		return doc_length;
	}

	public void setDoc_length(Long doc_length) {
		this.doc_length = doc_length;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getTitle() {
		return title;
	}
	public void setTitle(List<String> title) {
		this.title = title;
	}
	public List<String> getParagraph() {
		return paragraph;
	}

	public void setParagraph(List<String> paragraph) {
		this.paragraph = paragraph;
	}
	
	public HashMap<String, Long> getWordMap() {
		return wordMap;
	}
	

	public NewsArticle getOriginalArticle() {
		return originalArticle;
	}

	public void setOriginalArticle(NewsArticle originalArticle) {
		this.originalArticle = originalArticle;
	}


	public void WordFrequency() {
		List<String> content = new ArrayList<String>();
		if(this.title != null) content.addAll(this.title);
		if(content != null) content.addAll(this.paragraph);// the "content" is contents of the title and the paragraph
		for (String word : content) {//get wordMap for this artcile
            if (this.wordMap.containsKey(word)) {
                wordMap.put(word, wordMap.get(word) + (long)1);
            } else {
                wordMap.put(word, (long) 1);
            }
        }
		}
	

}
