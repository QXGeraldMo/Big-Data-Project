package uk.ac.gla.dcs.bigdata.studentfunctions.flatMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencyAccumulator;

/**
 * Qixiang Mo
 */

public class FrequencyZeroFilterMap implements FlatMapFunction<TermArticle,TermArticle>{
//so we want to filter the termArticle that has zero value for frquency.
	
	private static final long serialVersionUID = -5421918183346003486L;
	boolean frquencyZero;
	TermFrequencyAccumulator termFrequencyAccumulator;
	
	public FrequencyZeroFilterMap() {
		
	}
	

	public FrequencyZeroFilterMap(TermFrequencyAccumulator termFrequencyAccumulator) {
		super();
		this.termFrequencyAccumulator = termFrequencyAccumulator;
	}


	public Iterator<TermArticle> call(TermArticle value) throws Exception {
		
		frquencyZero = true;
		short frequency = value.getFrequency();
		Map<String, Integer> freqMap=new HashMap<String, Integer>();	
		
		if(frequency > 0) this.frquencyZero = false;
			
		
		if (!this.frquencyZero) {//if frequency is not 0 , then create a new TermArticle
			List<TermArticle> termArticleList = new ArrayList<TermArticle>(1);
			termArticleList.add(value); 
			freqMap.put(value.getTerm(), (int) frequency);
			termFrequencyAccumulator.add(freqMap);//this add function will get the same keys and add their value together
			return termArticleList.iterator(); 
		} else {//if frequency is not 0 , then return null
			List<TermArticle> termArticleList = new ArrayList<TermArticle>(0);
			return termArticleList.iterator();
		}
	}

}
