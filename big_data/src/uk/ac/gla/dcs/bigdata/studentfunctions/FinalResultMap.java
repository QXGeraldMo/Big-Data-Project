package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;


/**
 * Qixiang Mo
 * Ziyang Lin 
 * 
 */

public class FinalResultMap implements MapFunction<DocumentRanking , DocumentRanking >{

	/**
	 * 
	 */
	private static final long serialVersionUID = 100002131213L;

	@Override
	public DocumentRanking call(DocumentRanking value) throws Exception {
		// TODO Auto-generated method stub
		
		List<RankedResult> finalRankedResultList = new ArrayList<RankedResult>(10);				
		List<RankedResult> rankedResultList = value.getResults();
		Query query = value.getQuery();
		Iterator<RankedResult> rankedResultIterator = rankedResultList.iterator();
		
		while(rankedResultIterator.hasNext()) {
			RankedResult rankedResult = rankedResultIterator.next();
			NewsArticle article = rankedResult.getArticle();
			String title = article.getTitle();
			boolean flag = true;//True:keep this result, and vice versa.
			
			if(title==null) {// if the title is null ,then ignore it.
				continue;
			}
			
			if(finalRankedResultList.size()==0) {//if this is the first rankedResult, add it to the list
				finalRankedResultList.add(rankedResult);
				continue;
			}
			
			//if it is not the first, we will do follow action
			if((!finalRankedResultList.contains(rankedResult)) && finalRankedResultList.size()!=0) {
				for(RankedResult finalRankedResult:finalRankedResultList) {// get the result from list
					NewsArticle finalArticle = finalRankedResult.getArticle();
					String finalTitle = finalArticle.getTitle();
					if(title!=null) {
						double distance = TextDistanceCalculator.similarity(finalTitle, title);//calculate the distance
						if(distance<0.5) {
							flag = false;
							break;}}
					else {flag = false;}
				}
				
			}
			
			if(flag) {//flag = true ,so we add it into list
				if(finalRankedResultList.size()<10) {
					finalRankedResultList.add(rankedResult);
				}
			}		
			
		}
		
		DocumentRanking finalDocumentRanking =  new DocumentRanking(query,finalRankedResultList);
		return finalDocumentRanking;
	}

}

