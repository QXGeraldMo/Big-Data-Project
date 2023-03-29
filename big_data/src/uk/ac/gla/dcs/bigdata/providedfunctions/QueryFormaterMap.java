package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;


import org.apache.spark.util.CollectionAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;


/**Ziyang Lin : modified**/
public class QueryFormaterMap implements MapFunction<Row,Query> {

	private static final long serialVersionUID = 6475166483071609779L;

	private transient TextPreProcessor processor;

	CollectionAccumulator<String> allQueryTerms;

	public QueryFormaterMap(CollectionAccumulator<String> allQueryTerms){//get CollectionAccumulator<String> to get query List without duplicate
		this.allQueryTerms = allQueryTerms;
	}
	@Override
	public Query call(Row value) throws Exception {
	
		if (processor==null) processor = new TextPreProcessor();
		
		String originalQuery = value.mkString();
		System.out.println(originalQuery);
		
		List<String> queryTerms = processor.process(originalQuery);
		for(String a: queryTerms)
		allQueryTerms.add(a);
		
		short[] queryTermCounts = new short[queryTerms.size()];
		for (int i =0; i<queryTerms.size(); i++) queryTermCounts[i] = (short)1;
		
		Query query = new Query(originalQuery, queryTerms, queryTermCounts);

		//allQueryTerms.add(query.getQueryTerms());

		return query;
	}
}
