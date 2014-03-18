package com.piggybox.http;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a user agent (only mobile currently), classify the application as 'video',
 * 'music', 'social', 'im' and so on. Current 2014-03.
 * @version alpha
 * @author chenxm
 */
public class AppCategoryClassify extends SimpleEvalFunc<String> {
	public static Map<String, Pattern> regexMap;
	static{
		regexMap = new HashMap<String, Pattern>();
		regexMap.put("browsing", Pattern.compile("btwebclient|dolphin|ucweb|浏览器|(uc|qq|360|lb|miui|bidu)?browser|mobile\\s?(safari|maxthon)|iemobile"));
		regexMap.put("shopping", Pattern.compile("银行|必胜客|时光网|瑞丽网|聚(划算|便宜)|跳蚤街|韩都衣舍|购(物)?|淘(宝)?|易迅|淘伴|天猫|支付|商城|明星衣橱|唯品会|爱丽奢|当当|苏宁|京东|蘑菇|团|买|爱折客|肯德基|麦当劳|三叶草|优衣库|聚美|乐蜂|优惠|折扣|打折|返利|折800|vipshop|\\bMtime|onestore|shop|t(aobao|mall)|alipay|meilishuo|amazon|dangdang|ebay|suningebuy|tuan|51buy|mogujie|jumei|movieticket"));
		regexMap.put("video", Pattern.compile("优酷|看片|新浪(体育|NBA)|爱奇艺|第1体育|频道|公开课|云课堂|美剧|迅雷云|动画|风行|迅雷看看|百度hd|综艺|影音|视频|(快|直)播|影视|电驴大全|鲜果联播|鲨鱼土豆|곰플레이어|电影|电视|yingshi|shipin|baiduhd|\\b(TWC|qiyi|tex)|AppleCoreMedia|\\biku\\b|imdb|qvod|(qq|pp|虾米)live|pp(tv|stream)|ku(aibo|wo)|tudou|youku|funshion|sohu(kan)|(cc|hd|cn|le|haoe|dop|lig|bes|bn|mm|w\\.|i|cv|imgo|art)tv|tv(hd|show|live|series)|(m|kw|mx|km)player|video|movie[^(ticket)]"));
		regexMap.put("music", Pattern.compile("天天動聽|铃声|窄播|美乐时光|(酷|微|天天动|随心|爱)听|音(樂|乐|悦)(台|盒)?|音悦台|(电|電)台|歌|唱|baidubox|itunes|kugou|lessdj|(less|mini)lyrics|music|radio|(last|多听|虾米|豆瓣|凤凰|douban|meile|phoenix|kiss|my|yue|hot)\\.?FM|FM/|ktv"));
		regexMap.put("im", Pattern.compile("微(讯|语音)|蜂加|飞(聊|信)|电话|(有|私|易|微)信|米聊|阿里旺旺|旺旺(买|卖)家|陌陌|遇见|来往|\\b(sms|LINE[/\\s]\\d)|sms\\b|message|SayHi|\\bPath\\b|MSN|messenger|MiTalk|aliwangwang|MicroMessenger|momochat|skype|kakaotalk|(pad)?qq[/\\s2]|fetion|laiwang|whatsapp"));
		regexMap.put("social", Pattern.compile("ZAKER|堆糖|宽带山|小恩爱|易班|朋友|果壳|珍爱|婚恋|微(博|爱|格|发现|拍|聚|米)|随享|开心网|人人|校(友|内)|交友|啪啪|猫扑|(天涯)?社区|百科|某某|qq(空间|情侣)|豆瓣活动|辣妈帮|duitang|qqclient|instagram|weibo|weico|linkedin|xiaonei|renren|twitter|googleplus|qzone|zhihu|tieba|\\bTBClient|qiu(shi)?bai(ke)?"));
		regexMap.put("images", Pattern.compile("内涵段子|漫慢看|摄影|动漫|漫画|相机|壁纸|图片|美(图|拍)|表情|墙纸|照片|kuvva|wallpaper|tupian|camera|photo|pcviewer|emoji"));
		regexMap.put("news", Pattern.compile("(豆瓣|58)同城|搜房|安居客|新闻|今日头条| reuters|nytimes|cnbeta|iDaily|(ifeng|qq|sohu|smart|sina|bbc|jike)\\s?news|news\\s?(board|hd|ify|free|social)|iweekly|(time|mop)\\s?mobile|news|autohome|\\bYOKA"));
		regexMap.put("reading", Pattern.compile("时尚|(男|女|时)装|环球(人物|科学|银幕)|新浪视野|腾讯爱看|南方周末|书|阅|读|刊|[^预]报|杂志|小说|诗歌|文(学|集)|reader|[^b]read|epub|book|(insta|viva)Mag|百词斩|完美规划|shanbay|(单|拓)词|词汇|托福|雅思|(四|六)级|(口|美|英)语|考研"));
		regexMap.put("weather", Pattern.compile("天气|tianqi|weather|日历|calendar|rili365"));
		regexMap.put("p2p", Pattern.compile("torrent|µTorrent|bit(torrent|comet|spirit)|deluge|ppweb"));
		regexMap.put("maps", Pattern.compile("maps|地图|google(mobile|earth)|mobilemap|导航|地圖"));
		regexMap.put("mails", Pattern.compile("邮箱|(e|light)mail|mail"));
		regexMap.put("travel", Pattern.compile("航空|酒店|汽车|火车|飞机|旅(行|游|客)|穷游|游记|(门|车|机|订)票|景点|hotel|train|airplain|(打|租|列)车|地铁|在路上|去哪儿|咕咚运动"));
		regexMap.put("games", Pattern.compile("乐动|捕鱼达人|连连看|求合体|网游|游戏|快玩|钢铁侠|Kung\\sPow\\sGranny|linebubble|realracing|DespicableMe|UNOFriends|game|kuaiwan"));
		regexMap.put("fileshare", Pattern.compile("dropbox|storage|(快|云|酷|网)盘|yunpan|netdisk|百度文库|百度云"));
		regexMap.put("downloads", Pattern.compile("download|iosapp|appstore|PandaSpace|(apple|windows|google)\\s?store|searchapp|搜苹果|同步推|快装商店|PP助手|快用|限(时免费|免大师)|装机|管家"));
		regexMap.put("utilities", Pattern.compile("battery|电池|電池|文件管理|Yodao|youdao|有道|课程|设置|拨号|通讯录|手电筒|我查查|快递|大姨(吗|嗎)"));
		regexMap.put("update", Pattern.compile("update|upd|更新|升级"));
		regexMap.put("analytics", Pattern.compile("GoogleAnalytics|NetFox"));
		regexMap.put("ads", Pattern.compile("mobfox"));
		regexMap.put("compatable", Pattern.compile("\\b(mozilla|dalvik)/\\d|Apache-HttpClient"));
	}
	// Cache to store matched user agents.
	private Map<String, String> regexCache = new HashMap<String, String>();
	
	public String call(String useragent) {
		if ( useragent != null && useragent.length() > 0){
			// Use cache first.
			if ( regexCache.containsKey(useragent))
				return regexCache.get(useragent);
			// Regex matching
			for (String category : regexMap.keySet() ){
				Matcher matcher = regexMap.get(category).matcher(useragent);
				if ( matcher.find()){
					regexCache.put(useragent, category);
					return category;
				}
			}
		}
		return "unknown";
	}
}
