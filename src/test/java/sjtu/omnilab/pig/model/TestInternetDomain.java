package sjtu.omnilab.pig.model;

import com.google.common.net.InternetDomainName;

public class TestInternetDomain {
	public static void main(String[] args){
		System.out.println(InternetDomainName.from("baidu.com").isTopPrivateDomain());
	}
}
