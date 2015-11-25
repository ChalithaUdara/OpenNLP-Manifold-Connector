package org.apache.manifoldcf.agents.transformation.opennlp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;

public class OpenNlpExtractorConfig
{
	private static enum MODEL{
		SENTENCE, TOKENIZER, PEOPLE, LOCATIONS, ORGANIZATIONS;
	}
	
	// Specification nodes and values
    public static final String NODE_EXTRACT_PEOPLE = "ExtractPeople";
    public static final String NODE_EXTRACT_LOCATIONS = "ExtractLocations";
    public static final String NODE_EXTRACT_ORGANIZATIONS = "ExtractOrganizations";

    public static final String ATTRIBUTE_VALUE = "value";
    
    private static final String SENTENCE_DETECTOR_BIN = "resources/nlpmodels/en-sent.bin";
    private static final String TOKENIZER_BIN = "resources/nlpmodels/en-token.bin";
    private static final String NER_PEOPLE_BIN = "resources/nlpmodels/en-ner-person.bin";
    private static final String NER_LOCATION_BIN = "resources/nlpmodels/en-ner-location.bin";
    private static final String NER_ORG_BIN = "resources/nlpmodels/en-ner-organization.bin";
    
    private static SentenceModel sModel = null;
    private static TokenizerModel tModel = null;
    private static TokenNameFinderModel pModel = null;
    private static TokenNameFinderModel lModel = null;
    private static TokenNameFinderModel oModel = null;
    
    private static synchronized void initializeModel(MODEL m) throws InvalidFormatException, FileNotFoundException, IOException{
    	if(sModel == null && m == MODEL.SENTENCE)
    		sModel = new SentenceModel(new FileInputStream(SENTENCE_DETECTOR_BIN));
    	if(tModel == null && m == MODEL.TOKENIZER)
    		tModel = new TokenizerModel(new FileInputStream(TOKENIZER_BIN));
    	if(pModel == null && m == MODEL.PEOPLE)
    		pModel = new TokenNameFinderModel(new FileInputStream(NER_PEOPLE_BIN));
    	if(lModel == null && m == MODEL.LOCATIONS)
    		lModel = new TokenNameFinderModel(new FileInputStream(NER_LOCATION_BIN));
    	if(oModel == null && m == MODEL.ORGANIZATIONS)
    		oModel = new TokenNameFinderModel(new FileInputStream(NER_ORG_BIN));
    }
    
    public static final SentenceDetector sentenceDetector() throws InvalidFormatException, FileNotFoundException, IOException{
    	if(sModel == null)
    		initializeModel(MODEL.SENTENCE);
        return new SentenceDetectorME(sModel);
    }
    
    public static final Tokenizer tokenizer() throws InvalidFormatException, FileNotFoundException, IOException{
    	if(tModel == null)
    		initializeModel(MODEL.TOKENIZER);
        return new TokenizerME(tModel);
    }
    
    public static final NameFinderME peopleFinder() throws InvalidFormatException, FileNotFoundException, IOException{
    	if(pModel == null)
    		initializeModel(MODEL.PEOPLE);
        return new NameFinderME(pModel);
    }
    
    public static final NameFinderME locationFinder() throws InvalidFormatException, FileNotFoundException, IOException{
    	if(lModel == null)
    		initializeModel(MODEL.LOCATIONS);
        return new NameFinderME(lModel);
    }
    
    public static final NameFinderME organizationFinder() throws InvalidFormatException, FileNotFoundException, IOException{
    	if(oModel == null)
    		initializeModel(MODEL.ORGANIZATIONS);
        return new NameFinderME(oModel);
    }


    

}
