package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;
import java.util.List;

public class Query implements Serializable{

	private static final long serialVersionUID = 7309797023726062989L;
	
	String originalQuery;
	List<String> queryTerms;
	short[] queryTermCounts;
	
	public Query() {}

	public Query(String originalQuery, List<String> queryTerms, short[] queryTermCounts) {
		super();
		this.originalQuery = originalQuery;
		this.queryTerms = queryTerms;
		this.queryTermCounts = queryTermCounts;
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public void setOriginalQuery(String originalQuery) {
		this.originalQuery = originalQuery;
	}

	public List<String> getQueryTerms() {
		return queryTerms;
	}

	public void setQueryTerms(List<String> queryTerms) {
		this.queryTerms = queryTerms;
	}

	public short[] getQueryTermCounts() {
		return queryTermCounts;
	}

	public void setQueryTermCounts(short[] queryTermCounts) {
		this.queryTermCounts = queryTermCounts;
	}

	
	
	
	
	
	
}
