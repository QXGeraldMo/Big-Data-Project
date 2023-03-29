package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.util.AccumulatorV2;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.HashMap;
import java.util.Map;

public class ScoreAccumulator extends AccumulatorV2<Map<Tuple2<String,NewsArticle>, Double>, Map<Tuple2<String,NewsArticle>, Double>> {

    /**
	 * QixiangMo modifed based on AccumulatorV2
	 */
	private static final long serialVersionUID = 112312362343213L;
	
	private Map<Tuple2<String,NewsArticle>,  Double> map = new HashMap<>();

    public ScoreAccumulator() {
    }

    public ScoreAccumulator(Map<Tuple2<String,NewsArticle>, Double> map) {
        this.map = map;
    }

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<Tuple2<String,NewsArticle>,Double>, Map<Tuple2<String,NewsArticle>,Double>> copy() {
        return new ScoreAccumulator(new HashMap<>(map));
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(Map<Tuple2<String,NewsArticle>, Double> values) {
        for (Map.Entry<Tuple2<String,NewsArticle>, Double> entry : values.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void merge(AccumulatorV2<Map<Tuple2<String,NewsArticle>, Double>, Map<Tuple2<String,NewsArticle>, Double>> other) {
        Map<Tuple2<String,NewsArticle>, Double> otherMap = other.value();
        for (Map.Entry<Tuple2<String,NewsArticle>, Double> entry : otherMap.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public Map<Tuple2<String,NewsArticle>, Double> value() {
        return map;
    }
}
