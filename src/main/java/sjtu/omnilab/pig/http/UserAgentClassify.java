package sjtu.omnilab.pig.http;

import sjtu.omnilab.pig.utils.SimpleEvalFunc;

/**
 * Given a user agent string, this UDF classifies clients to 'mobile' and 'desktop'.
 */
public class UserAgentClassify extends SimpleEvalFunc<String>
{
  
  public String call(String useragent)
  {
    String ua=useragent.toLowerCase();
    if(ua.matches(MobileType.MOB_STRING))
    	return "mobile";
    else
    	return "desktop";     
  }
}

