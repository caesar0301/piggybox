package com.piggybox.omnilab;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a AP name string, this function return the description of the building.
 * Specifically for AP name, both full name string (the default mode, e.g. BYGTSG-4F-01) and
 * building name (e.g. BYGTSG) can be used.
 * If only building name is given, you can save processing time to declare this method with @full_apname param.
 * But for accuracy, the full AP name is prefered.
 * <p>
 * For example:
 * <pre>
 * {@code
 * DEFINE getAPBuildingInfo com.piggybox.omnilab.getAPBuildingInfo();
 * DEFINE getBuildingInfo com.piggybox.omnilab.getAPBuildingInfo(false); // use @full_apname param
 * 
 * -- input: 
 * -- (BYGTSG-1F-01)
 * input = LOAD 'input' AS (apname:CHARARRAY);
 * 
 * -- output: 
 * -- ((包玉刚图书馆, LibBldg, PUB))
 * output = FOREACH input GENERATE getAPBuildingInfo(apname);
 * 
 * 
 * -- input: 
 * -- (BYGTSG)
 * input = LOAD 'input' AS (buildname:CHARARRAY);
 * 
 * -- output: 
 * -- ((包玉刚图书馆, LibBldg, PUB))
 * output = FOREACH input GENERATE getBuildingInfo(buildname);
 * } 
 * </pre>
 * </p>
 * @author chenxm
 */
public class GetAPBuildingInfo extends SimpleEvalFunc<Tuple>{
	private static final String AP_BUILDING_DATABASE = "/APNames-UTF8.yaml";
	private Map<String, Map<String, String>> APNameDB;
	private boolean full_apname = true;
	private Map<String, String> APBN_RealBN_Cache = new HashMap<String, String>();
	
	public GetAPBuildingInfo(){
		this(GetAPBuildingInfo.class.getResourceAsStream(AP_BUILDING_DATABASE), true);
	}
	
	public GetAPBuildingInfo(boolean full_apname){
		this(GetAPBuildingInfo.class.getResourceAsStream(AP_BUILDING_DATABASE), full_apname);
	}
	
	@SuppressWarnings("unchecked")
	public GetAPBuildingInfo(InputStream APDBYAML, boolean full_apname) {
		this.full_apname = full_apname;
		// Load yaml database
		Yaml yaml = new Yaml(new SafeConstructor());
		Map<String,Object> regexConfig = (Map<String,Object>) yaml.load(APDBYAML);
	    APNameDB = (Map<String, Map<String, String>>) regexConfig.get("apprefix_sjtu");
	    //System.out.println(APNameDB);
	}

	public Tuple call(String APName){
		// Default result
		Tuple result = TupleFactory.getInstance().newTuple();
		result.append(null);
		result.append(null);
		result.append(null);
		if ( APName == null ) return result;
		
		if ( full_apname )  { // Given full AP name string
			String[] parts = APName.split("-\\d+F-", 2);
			String buildName = parts[0];
			// Remove MH- prefix
			if (buildName.startsWith("MH-"))
				buildName = buildName.substring(3, buildName.length());
			// Check cache first
			if ( APBN_RealBN_Cache.containsKey(buildName) ) { // Cache hit
				String cacheRealBN = APBN_RealBN_Cache.get(buildName);
				result = getBuildInfo(cacheRealBN);
			} else { // Cache miss
				if ( APNameDB.containsKey(buildName)) { // Building name found
					result = getBuildInfo(buildName);
					APBN_RealBN_Cache.put(buildName, buildName); // Cache the real building name
				} else { // Worst case; try to find its longest matched building name
					String realBuildName = null;
					for ( String BN : APNameDB.keySet())
						if ( buildName.contains(BN) )
							if ( realBuildName == null )
								realBuildName = BN;
							else if ( BN.length() > realBuildName.length() ) // Get the longest match
								realBuildName = BN;
					if ( realBuildName != null ){
						result = getBuildInfo(realBuildName);
						APBN_RealBN_Cache.put(buildName, realBuildName); // Cache the real building name
					}
				}
			}
		} else { // Given build name, skip cache actions
			if ( APNameDB.containsKey(APName) ) // Have item
				result = getBuildInfo(APName);
		}
		return result;
	}
	
	private Tuple getBuildInfo(String realBuildName){
		Tuple result = TupleFactory.getInstance().newTuple();
		Map<String, String> buildInfo = APNameDB.get(realBuildName);
		result.append(buildInfo.get("dsp"));
		result.append(buildInfo.get("typ"));
		result.append(buildInfo.get("usr"));
		return result;
	}
}
