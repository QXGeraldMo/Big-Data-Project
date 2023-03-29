package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;


/**
 * Jingyi Mao
 */
public class DocLengthMap implements MapFunction<NewsArticlesCleaned, Long>{

 
	
	private static final long serialVersionUID = -6249869268110115686L;

	@Override
	public Long call(NewsArticlesCleaned value) throws Exception {
		return value.getDoc_length();
	}

}
