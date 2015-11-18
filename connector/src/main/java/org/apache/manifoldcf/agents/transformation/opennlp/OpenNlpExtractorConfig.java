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
    
    public static final SentenceDetector sentenceDetector() throws InvalidFormatException, FileNotFoundException, IOException{
        return new SentenceDetectorME(new SentenceModel(new FileInputStream(SENTENCE_DETECTOR_BIN)));
    }
    
    public static final Tokenizer tokenizer() throws InvalidFormatException, FileNotFoundException, IOException{
        return new TokenizerME(new TokenizerModel(new FileInputStream(TOKENIZER_BIN)));
    }
    
    public static final NameFinderME peopleFinder() throws InvalidFormatException, FileNotFoundException, IOException{
        return new NameFinderME(new TokenNameFinderModel(new FileInputStream(NER_PEOPLE_BIN)));
    }
    
    public static final NameFinderME locationFinder() throws InvalidFormatException, FileNotFoundException, IOException{
        return new NameFinderME(new TokenNameFinderModel(new FileInputStream(NER_LOCATION_BIN)));
    }
    
    public static final NameFinderME organizationFinder() throws InvalidFormatException, FileNotFoundException, IOException{
        return new NameFinderME(new TokenNameFinderModel(new FileInputStream(NER_ORG_BIN)));
    }


    

}
