package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * 正则
 * @author Administrator
 *
 */

public class regix {
  public static void main(String[] args) {
//	String yuming="www.baidu.com";
//	String[] ym=yuming.split("\\.");
//	for(String m:ym){
//		System.out.println(m);
//	}
//	String yuming1="www\\baidu\\com";
//	String[] ym1=yuming1.split("\\\\");
//	for(String m:ym1){
//		System.out.println(m);
//	}
	  //()表示组在正则表达式中会自动给组编号从1开始分几组看左括号个数从左到右编号
	  //取组\加组号
	 /* String str="saabccdeef";
	  String[] s=str.split("(.)\\1");
	  for(String m:s){
			System.out.println(m);
     }*/
	  /*String str="saabcccdeef";
	  String[] s=str.split("(.)\\1+");
	  for(String m:s){
			System.out.println(m);
     }*/
	  //叠字替换&，单个字符
	  /*String str="saabcccdeef";
	  String s=str.replaceAll("(.)\\1+", "&");
			System.out.println(s); 
	  //后面想要使用前面组的内容使用$
	  String s=str.replaceAll("(.)\\1+", "$1");
		System.out.println(s);*/
	  
	  //获取连续三个或者三个以上的字母
	 /* String test="xxx1ss2www3";
	  String reg="[a-zA-Z]{3,}";
	  Pattern p=Pattern.compile(reg);
	  Matcher m=p.matcher(test);//返回对象匹配器
	//  System.out.println(m.matches()) ;//false
	 // System.out.println(m.find());;//查找是否有匹配对象
	  while(m.find())		  
	   System.out.println(m.group());;*///{1,3}
	   String str="112222333@qq.com$$asdf@163.com";
	   String regEx = "[1-9a-zA-Z_]{1,}[0-9]{0,}@(([a-zA-z0-9]*){1,}\\.)[1-9a-zA-z\\-]{1,}";
	    // 编译正则表达式
	    Pattern pattern = Pattern.compile(regEx);
	    // 忽略大小写的写法
	    // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
	    Matcher m = pattern.matcher(str);
	    // 字符串是否与正则表达式相匹配
	    while(m.find())		  
	 	   System.out.println(m.group());
			   
  }
}
