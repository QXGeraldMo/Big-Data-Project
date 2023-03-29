package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


/**
 * Qixiang Mo
 * Jingyi Mao 
 */

public class TermArticle implements Serializable{
	private static final long serialVersionUID = 7860296794078492249L;
	
	
	String term;
	NewsArticlesCleaned article;
	short frequency = 0;
	
	public TermArticle() {
	}
	
	public TermArticle(String term, NewsArticlesCleaned article) {
		super();
		this.term = term;
		this.article = article;
		setFrequency();//get the word map after text processing
	}
	

	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public NewsArticlesCleaned getArticle() {
		return article;
	}
	public void setArticle(NewsArticlesCleaned article) {
		this.article = article;
	}
	
	public short getFrequency() {
		return frequency;
	}

	private void setFrequency() {//generate a word map for this article
		HashMap<String, Long> wordMap = article.getWordMap();
		if(wordMap.containsKey(term)) {
			this.frequency = wordMap.get(term).shortValue();
		}
		else {
		this.frequency = 0;}
	}
	
}

