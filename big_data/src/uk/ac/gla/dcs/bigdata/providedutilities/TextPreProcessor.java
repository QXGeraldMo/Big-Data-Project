package uk.ac.gla.dcs.bigdata.providedutilities;

import java.util.ArrayList;
import java.util.List;

import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

/**
 * This class provides pre-processing for text strings based on the Terrier IR platform.
 * In particular, it provides an out-of-the-box function for performing stopword removal
 * and stemming on a text string.
 * 
 * @author Richard
 *
 */


//result will be Processed Tokens: [quick, brown, fox, jump, over, lazi, dog] if we enter "The quick brown fox jumps over the lazy dog"
public class TextPreProcessor {

	
	BaseTermPipelineAccessor termProcessingPipeline; // processes an individual term
	Tokeniser tokeniser; // splits a string into multiple terms
	
	/**
	 * Default Constructor
	 */
	public TextPreProcessor() {
		
		termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords","PorterStemmer");
		tokeniser = Tokeniser.getTokeniser();
		
	}
	
	/**
	 * Returns an array of processed terms for an input text string
	 * @param text
	 * @return
	 */
	public List<String> process(String text) {
		String[] inputTokens = tokeniser.getTokens(text);
		
		if (inputTokens==null) return new ArrayList<String>(0);
		
		List<String> outTokens = new ArrayList<String>(inputTokens.length);
		for (int i =0; i<inputTokens.length; i++) {
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);
			if (processedTerm==null) continue;
			outTokens.add(processedTerm);
		}
		
		return outTokens;
		
	}
	
	
    public static void main(String[] args) {
        
        // create an instance of the TextPreProcessor class
        TextPreProcessor preprocessor = new TextPreProcessor();
        
        // define some sample text to preprocess
        String sampleText = "The quick brown fox jumps over the lazy dog";
        
        // preprocess the sample text using the TextPreProcessor class
        List<String> processedTokens = preprocessor.process(sampleText);
        
        // print the processed tokens
        System.out.println("Processed Tokens: " + processedTokens);
        
    }
	
}

