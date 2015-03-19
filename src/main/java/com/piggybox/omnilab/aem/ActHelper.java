package com.piggybox.omnilab.aem;

import java.util.LinkedList;
import java.util.List;

class ActHelper {
    String aid;
    List<String> urls;

    public ActHelper(String aid, String url){
        this.aid = aid;
        this.urls = new LinkedList<String>();
        this.urls.add(url);
    }

    public void addRequst(String url){
        this.urls.add(url);
    }

    public boolean isEqual(ActHelper a){
        if ( this.urls.get(0).equals(a.urls.get(0)))
            return true;
        return false;
    }

    public int size(){
        return this.urls.size();
    }

    public String assembleUrls(){
        String res = "";
        for (String url : this.urls){
            res += url;
            res += ";";
        }
        return res;
    }
}