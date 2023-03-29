package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;

public class DocRankMap implements MapFunction<Query, DocumentRanking>{


	/**
	 * Jingyi Mao
	 * Qixiang Mo modified
	 */
	private static final long serialVersionUID = 3490083426550218984L;
	
	
	Broadcast<Map<Tuple2<String,NewsArticle>, Double>> broadcastScoreMap;
	
	
	public DocRankMap(Broadcast<Map<Tuple2<String,NewsArticle>, Double>> broadcastScoreMap) {
		this.broadcastScoreMap = broadcastScoreMap;
	}
	

	@Override
	public DocumentRanking call(Query value) throws Exception {
		

		HashMap<NewsArticle, Double> dphmap = new HashMap<>();
		List<RankedResult> docranks = new ArrayList<>();
		List<String> queryTerms = value.getQueryTerms();
		int term_num = queryTerms.size();
	
		
		//iterate the scoreMap to get each <term-doc>-score pair
		 Map<Tuple2<String,NewsArticle>, Double> scoreMap = broadcastScoreMap.value();
		 for (Map.Entry<Tuple2<String, NewsArticle>, Double> entry : scoreMap.entrySet()) {
			    Tuple2<String, NewsArticle> key = entry.getKey();
			    Double score = entry.getValue();
			    String cur_term = key._1();
			    NewsArticle cur_doc = key._2();
			    if(queryTerms.contains(cur_term)) {
					if(dphmap.containsKey(cur_doc)) {
						dphmap.put(cur_doc, dphmap.get(cur_doc) + score);
					}else {
						dphmap.put(cur_doc, score);
					}
				}
			  
			}
		 
		
		
		//rank document
		
		List<NewsArticle> collect = dphmap.entrySet().stream().filter(x -> x.getValue() != null).sorted((o1, o2) -> {
            if (o1.getValue() < o2.getValue()) {
                return 1;
            }
            return -1;
        }).map(x -> x.getKey()).collect(Collectors.toList());
		
		for(NewsArticle doc: collect) {
			String docid = doc.getId();
			NewsArticle article = doc; 
			double score = dphmap.get(doc) / term_num;
			RankedResult rankres = new RankedResult(docid, article, score);
			docranks.add(rankres);
		}

		
		DocumentRanking res = new DocumentRanking(value, docranks);

		
		return res;
		
		
		

		
	}

}