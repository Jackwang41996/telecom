package Tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class MProperties {
	static Properties p = new Properties(); 
	static{  
        try {    		
    		InputStream is=MProperties.class.getResourceAsStream("/conf.properties");    
            BufferedReader br=new BufferedReader(new InputStreamReader(is));    
    		p.load(br);   		 
        } catch (IOException e) {  
            e.printStackTrace();   
        }  
    }  
    /** 
     * 根据key得到value的值 
     */  
    public static String getValue(String key)  
    {  
        return p.getProperty(key);  
    } 	
	
}

