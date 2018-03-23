package Personas;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import CountMatch.ProductIdmatch;
import CountMatch.ProductIdmatch.mapper;
import Tools.MProperties;

public class Personas {
	/**
	 * 关联行为分类数据统计用户各分类情况。
	 * 用户    某个行为次数    行为分类总次数
	 * 用户id 行为分类id 行为分类次数  总次数  标准值
	 * @param args
	 */
	//
	//t_dx_basic_classify_link.txt
	//00001|041401000000000000
	//行为分类id 行为id
	//4510002507,001914000000000000,20
	/**
	 * map端以分类id分组，用户id+访问次数为value，并对map端数据进行累加
	 * */
	public static class PersonaMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		//分类关系地址库
		private Map<String, String> typeData = new HashMap<String,String>();
		//存储每个map数据中用户分类的记录数
		private Map<String, Integer> userType = new HashMap<String,Integer>();		
		private String typeKey ;		
		//在map执行前预先加载行为id与分类id对应关系数据，并通过hashmap存储。
		@Override
		protected void setup( Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
			BufferedReader reader = new BufferedReader(new FileReader("t_dx_basic_classify_link.txt"));
			String str = null;
			try {
				// 一行一行读取
				while ((str = reader.readLine()) != null) {
					// 对缓存中的表进行分割
					String[] splits = str.split(MProperties.getValue("msgaddrspilt"));
					//行为ID，分类ID
					typeData.put(splits[1], splits[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				reader.close();
			}
		}
		//map端累计用户分类访问次数
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//4510002507,001914000000000000,20
			String[] values = value.toString().split(MProperties.getValue("outsplit"));
			//是否包含行为ID对应的分类ID
			if(typeData.containsKey(values[1])){
				//key：分类id+用户ID
				typeKey = typeData.get(values[1]) +"," + values[0];					
				if(userType.containsKey(typeKey)){
					//累加key的访问次数
					userType.put(typeKey, userType.get(typeKey) + Integer.parseInt(values[2]));
				}else{
					//加载key的访问次数
					userType.put(typeKey, Integer.parseInt(values[2]));
				}
			}
		}
		//clean方法在map统计之后执行，在此方法中输出map方法累加数据结果
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
	        for (String key : userType.keySet()) {
	        	//分类id
	        	k = new Text(key.split(",",-1)[0]);
	        	v= new Text(key.split(",",-1)[1] + MProperties.getValue("outsplit") + userType.get(key));
	        	//key：分类ID；value：用户ID，访问次数
	        	context.write(k , v);
	        }
		}
	}
	/**
	 * 以分类id分组统计用户每个分类的访问次数及分类的总访问次数，并将数据标准化。
	 * */
	public static class TopNReducer extends Reducer<Text, Text, NullWritable, Text> {
		private Text result = new Text();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			//总访问数
			int pv = 0;
			//人数
			int n = 0;
			//数据迭代
			Map<String, Integer> map = new HashMap();
			//计算分类总pv
		    for (Text val : values) {
		    	String[] str = val.toString().split(MProperties.getValue("outsplit"));
		    	//总访问数
		    	pv = pv + Integer.parseInt(str[1]);

		    	if(map.containsKey(str[0])){
		    		//用户访问数
		    		map.put(str[0], map.get(str[0]) + Integer.parseInt(str[1]) );
		    	}else{
		    		//用户访问数
		    		map.put(str[0], Integer.parseInt(str[1]));
		    		//人数
		    		n = n +1;
		    	}
		    }
		    
		    //=========z-score标准差计算（反映数据集的离散程度，公式：（原数据-均值）/标准差）===========
		    //平方累加和
		    double math2 = 0;
		    //均值
		    double avg = pv/n ;
		    //方差分母
		    for(String s : map.keySet()){
		    	//math2=数据-均值后的平方累加和，
		    	math2 = math2 + Math.pow(map.get(s)-avg,2);
		    }
		    //标准差(方差开根号，方差=math2/数据条数)
		    double fc = Math.sqrt(math2/n);	    
		    //循环用户列表 
		    for(String skey : map.keySet()){
		    	//z-score 标准化,（原数据-均值）/标准差
		    	double b = (map.get(skey)-avg)/fc;
		    	//数据平移
		    	b = b + 5;
		    	//上限范围10
		    	if( b > 10){
		    		b = 10 ;
		    	}
		    	//下限范围0
		    	if(b < 0){
		    		b = 0;
		    	}
		    	//用户ID，分类ID，用户分类pv，分类总体pv，标准值
		    	result = new Text(skey + MProperties.getValue("outsplit")
		    			+ key.toString() + MProperties.getValue("outsplit")
		    			+ map.get(skey) + MProperties.getValue("outsplit")
		    			+ pv + MProperties.getValue("outsplit") 
		    			+  b); 
		    	context.write(NullWritable.get(), result);
		    }
		}
	}
	
public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf);
	job.setJarByClass(Personas.class);
	job.setMapperClass(PersonaMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(TopNReducer.class);
	//输出格式
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);						
	job.addCacheFile(new URI("hdfs://192.168.245.20:9000/telecom/config/t_dx_basic_classify_link.txt"));			
	FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data3"));
	Path out=new Path("hdfs://192.168.245.20:9000/telecom/data9");			
	FileOutputFormat.setOutputPath(job, out);
	boolean res=job.waitForCompletion(true);
}
}
