package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.CollectionAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatMap.FrequencyZeroFilterMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatMap.TermArticleMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.reducor.DocLengthSumReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencyAccumulator;


/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {


	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}


	}



	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		
		CollectionAccumulator<String> allQueryTerms = spark.sparkContext().collectionAccumulator();//Read all query term together as a String List
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(allQueryTerms), Encoders.bean(Query.class)); // this converts each row into a Query
		queries.show();
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		//Convert Query List to one dimension List
		System.out.println("we are converting query into query List");
		Set<String> allQueryTermsToSet = new HashSet<>();   //delete duplicate element
		allQueryTermsToSet.addAll(allQueryTerms.value());//convert query into Set, remove duplicated terms
		List<String> allQueryTermsToList = new ArrayList<String>(allQueryTermsToSet);//Convert back to List
		
		//convert article into NewsArticlesCleaned, NewsArticlesCleaned is a article-related class type after text processing
		Encoder<NewsArticlesCleaned> newsArticleEncoder = Encoders.bean(NewsArticlesCleaned.class);
		Dataset<NewsArticlesCleaned> articles = news.map(new NewsProcessorMap(), newsArticleEncoder);

		//get averageDocumentLengthInCorpus
		long totalDocsInCorpus = articles.count();//totalDocsInCorpus
		System.out.println(totalDocsInCorpus);
		Dataset<Long> docLength = articles.map(new DocLengthMap(), Encoders.LONG());//Map article doc_length to a docLengthMap
		long docLengthSUM = docLength.reduce(new DocLengthSumReducer()); //sum  all the document Length together
		double averageDocumentLengthInCorpus = docLengthSUM / totalDocsInCorpus;//get average Document Length
		
		//Define an accumulator to calculate the total term and frequency
		TermFrequencyAccumulator termFrequencyAccumulator = new TermFrequencyAccumulator(new HashMap<>());
		spark.sparkContext().register(termFrequencyAccumulator, "termFrequencyAccumulator");
		
		//Broadcast allQueryTermsToList
		System.out.println("we are broadcasting！！！！！");
		//Broadcast allQueryTermsToList, which is a list of all query terms without duplicate
		Broadcast<List<String>> broadcastAllQueryTermsToList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(allQueryTermsToList);

		//Broadcast broadcastTotalDocsInCorpus and broadcastAverageDocumentLengthInCorpus
		Broadcast<Long> broadcastTotalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> broadcastAverageDocumentLengthInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		
		//Term-article-map 
		System.out.println("we are maping new to termArticle！！！！！");
		Encoder<TermArticle> termArticleEncoder= Encoders.bean(TermArticle.class);
		
		//Convert NewsArtcilesCleaned to TermArticles, use broadcastAllQueryTermsToList to get each term in Query
		Dataset<TermArticle> termArtcles = articles.flatMap(new TermArticleMap(broadcastAllQueryTermsToList), termArticleEncoder);
		System.out.println("termArticle numbers before zero frquency filtering :" + termArtcles.count());
	
		//Zero frequency filter， This is a very important step to optimize, we ignore the term-article pair that has
		//zero frequency value and map the pairs having frequency values to a new Dataset<TermArticle> filteredTermArtcles
		System.out.println("we are filtering termArticle that has zero frequency！！！！");
		FrequencyZeroFilterMap frquencyZeroFilter = new FrequencyZeroFilterMap(termFrequencyAccumulator); 
		Dataset<TermArticle> filteredTermArtcles = termArtcles.flatMap(frquencyZeroFilter,termArticleEncoder);
		System.out.println("TermArticle numbers after filering:" + filteredTermArtcles.count());//get the count after filtering
	

		//broadcast TermandFrequency map to calculate DPH score
		Map<String, Integer> termFrequencyMap = termFrequencyAccumulator.value();
		Broadcast<Map<String, Integer>> broadcastTermFrequencyMap = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termFrequencyMap);
		System.out.println(termFrequencyMap);
		
		
		//Score Accumulator, just as same as termArticleDPH. Making this Accumulator is to make sure we can broadcast to the DocRankMap
		//cause Dataset cannot be broadcast, cause termArticleDPH.collectASlist() will cause missing fields. 
		ScoreAccumulator scoreAccumulator = new ScoreAccumulator(new HashMap<>());
		spark.sparkContext().register(scoreAccumulator, "scoreAccumulator");
		
		///DPH term-Article, we map the filteredTermArticle to termArticleDPH map
		System.out.println("We are calculating DPH score！！！！！！");
		Encoder<TermArticleDPH> dphEncoder = Encoders.bean(TermArticleDPH.class);
		Dataset<TermArticleDPH> termArticleDPH = filteredTermArtcles.map(new DPHcalculatorMap(broadcastTermFrequencyMap,broadcastTotalDocsInCorpus,
												broadcastAverageDocumentLengthInCorpus, scoreAccumulator), dphEncoder);	
		termArticleDPH.count();
		
		
		//create score Accumulator is just as same as termArticleDPH
		Map<Tuple2<String,NewsArticle>, Double> scoreMap = scoreAccumulator.value();
		Broadcast<Map<Tuple2<String,NewsArticle>, Double>> broadcastScoreMap = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(scoreMap);
		
		//it's a final step to get average score and rank them
		System.out.println("We are combining the term into query and we are ranking!!!！！");
		DocRankMap docrankmap = new DocRankMap(broadcastScoreMap);
		Dataset<DocumentRanking> docrank = queries.map(docrankmap, Encoders.bean(DocumentRanking.class)); 
		
		
		//get the final result
		System.out.println("We are going to rank the final result");
		Dataset<DocumentRanking> finalDocRank = docrank.map(new FinalResultMap(), Encoders.bean(DocumentRanking.class)); 
		System.out.println(finalDocRank.count());
		List<DocumentRanking> finalDocRankList = finalDocRank.collectAsList();
		
		
		
		return finalDocRankList ; // replace this with the the list of DocumentRanking output by your topology
	}


}



