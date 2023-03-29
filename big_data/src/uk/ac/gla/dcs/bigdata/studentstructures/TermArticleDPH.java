package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Qixiang Mo
 * Ziyang Lin 
 */


public class TermArticleDPH implements Serializable{
	
	private static final long serialVersionUID = 7329797824926066989L;
	
	double DPHscore;
	String term;
	NewsArticle article;
	
	
	public  TermArticleDPH () {}
	
	
	public  TermArticleDPH (double dPHscore, String term, NewsArticle  article) {
		super();
		this.DPHscore = dPHscore;
		this.term = term;
		this.article = article;
	}

	public double getDPHscore() {
		return DPHscore;
	}
	public void setDPHsocre(double dPHscore) {
		DPHscore = dPHscore;
	}
	public String getTerm() {
		return term;
	}
	public void setTerm(String terms) {
		this.term = terms;
	}
	public NewsArticle getArticle() {
		return article;
	}
	public void setArticle(NewsArticle  article) {
		this.article = article;
	}

	
}