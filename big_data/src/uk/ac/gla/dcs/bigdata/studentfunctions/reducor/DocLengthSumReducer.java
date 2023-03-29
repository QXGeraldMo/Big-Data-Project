package uk.ac.gla.dcs.bigdata.studentfunctions.reducor;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;

/**
 * Jingyi Mao
 */
public class DocLengthSumReducer implements ReduceFunction<Long>{


	/**
	 * calculate the sum of the doc length
	 */
	private static final long serialVersionUID = 5890430376953697462L;

	@Override
	public Long call(Long v1, Long v2) throws Exception {
		return v1+v2;
	}//this function will recursively apply to every pair of value in that list entill reduce to a single value

}