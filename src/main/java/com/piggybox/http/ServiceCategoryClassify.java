package com.piggybox.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a host string, this function return the host category as long as the class information.
 * <p>
 * For example:
 * <pre>
 * {@code
 * DEFINE ServiceCategoryClassify com.piggybox.http.ServiceCategoryClassify();
 * 
 * -- input: 
 * -- (www.qq.com)
 * input = LOAD 'input' AS (host:CHARARRAY);
 * 
 * -- output: 
 * -- (腾讯网;3;109)
 * output = FOREACH input GENERATE ServiceCategoryClassify(host); 
 * } 
 * </pre>
 * </p>
 * @author chenxm
 */
public class ServiceCategoryClassify extends SimpleEvalFunc<String> {

	private static final String REGEX_HOST_CATEGORY = "/host-regexes.yaml";
	private List<HostPattern> hostParser = new LinkedList<HostPattern>();
	private Map<String, Map<String, Integer>> categoryClassesParser;
	private Map<Integer, String> subCategory1Parser;
	private static Map<String, String> categoryCache = new HashMap<String, String>();

	public ServiceCategoryClassify() throws IOException {
		this(ServiceCategoryClassify.class.getResourceAsStream(REGEX_HOST_CATEGORY));
	}

	@SuppressWarnings("unchecked")
	public ServiceCategoryClassify(InputStream regexYaml) {
		// Initialize the classification engine
		Yaml yaml = new Yaml(new SafeConstructor());
	    Map<String,Object> regexConfig = (Map<String,Object>) yaml.load(regexYaml);
	    List<Map<String, String>> hostRegexes = (List<Map<String, String>>) regexConfig.get("host_parser");
	    //System.out.println(hostRegexes);
	    for(Map<String, String> hostRegex : hostRegexes)
	    	hostParser.add(new HostPattern(hostRegex.get("regex"), hostRegex.get("category")));
	    categoryClassesParser = (Map<String, Map<String, Integer>>) regexConfig.get("category_classes");
	    subCategory1Parser = (Map<Integer, String>) regexConfig.get("cls1_map");
	}
	
	/**
	 * Main call method for this evaluation function.
	 * @param hostString
	 * @return
	 */
	public String call(String hostString) {
		if ( hostString == null )
			return "unknown;"+getCategoryClasses(null);
		
		// Facilitate cache
		if ( categoryCache.containsKey(hostString) ){
			String category = categoryCache.get(hostString);
			return String.format("%s;%s", category, getCategoryClasses(category));
		}
		
		boolean matched = false;
		HostPattern pattern = null;
		for ( HostPattern p : hostParser ){
			if ( p.ifmatch(hostString)){
				matched = true;
				pattern = p;
				break;
			}
		}
		if (matched){
			String category = pattern.getCategory();
			categoryCache.put(category, getCategoryClasses(category));
			return String.format("%s;%s", category, getCategoryClasses(category));
		}
		return "unknown;"+getCategoryClasses(null);
	}
	
	/**
	 * Get class information for given category string.
	 * @param category
	 * @return
	 */
	private String getCategoryClasses(String category){
		if (category != null && categoryClassesParser.containsKey(category)){
			Map<String, Integer> classes = categoryClassesParser.get(category);
			Integer cat1 = classes.get("cls1");
			Integer cat2 = classes.get("cls2");
			if (subCategory1Parser.containsKey(cat1)){
				String cat1Sematic = subCategory1Parser.get(cat1);
				return String.format("%s;%d", cat1Sematic, cat2);
			} else 
				return String.format("%d;%d", cat1, cat2);
		}
		return "unknown;unknown"; // Default value when there is no registered category.
	}
	
	/**
	 * Inner class for convenient usage of host regex patterns.
	 * @author chenxm
	 *
	 */
	protected static class HostPattern {
		private Pattern pattern = null;
		private String category = null;
		
		public HostPattern(String regex, String cat){
			pattern = Pattern.compile(regex);
			category = cat;
		}
		
		public boolean ifmatch(String givenhost){
			if ( givenhost == null )
				return false;
			Matcher matcher = pattern.matcher(givenhost);
			return matcher.find() ? true : false;
		}
		
		public String getCategory(){
			return category;
		}
	}
}
