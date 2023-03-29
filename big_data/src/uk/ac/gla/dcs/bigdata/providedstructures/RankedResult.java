package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;


public class RankedResult implements Serializable, Comparable<RankedResult> {

	private static final long serialVersionUID = -2905684103776472843L;
	
	String docid;
	NewsArticle article; 
	double score;
	
	public RankedResult() {}
	
	public RankedResult(String docid, NewsArticle article, double score) {
		super();
		this.docid = docid;
		this.article = article;
		this.score = score;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(RankedResult o) {
		return new Double(score).compareTo(o.score);
	}
	
	
	  public static void main(String[] args) throws Exception {
		  RankedResult r1 = new RankedResult();
		  RankedResult r2 = new RankedResult();
		  
		  r1.setScore(2);
		  r2.setScore(1);
		  System.out.println(r1.compareTo(r2));
	  }
	
	
}
