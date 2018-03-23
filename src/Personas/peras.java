package Personas;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.BR;
import Tools.MProperties;

public class peras {
 public static class mapper extends Mapper<LongWritable, Text, Text, Text>{
	 private Text k = new Text();
		private Text v = new Text();
		Map<String, String> mapclassid = new HashMap<String,String>();
		@Override
		protected void setup( Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
			BufferedReader br = new BufferedReader(new FileReader("t_dx_basic_classify_link.txt"));
			String str = null;
			while ((str = br.readLine()) != null) {
				// 对缓存中的表进行分割
				String[] splits = str.split(MProperties.getValue("msgaddrspilt"));
				//行为ID，分类ID
				mapclassid.put(splits[1], splits[0]);
			}
		  br.close();
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//4510002507,001914000000000000,20
			String[] values = value.toString().split(MProperties.getValue("outsplit"));
			if(mapclassid.containsKey(values[1])){
				k.set(mapclassid.get(values[1]));
				v.set(values[0]+","+values[2]);
				//key：分类ID；value：用户ID，访问次数
	        	context.write(k , v);
			}
		}		
        }
	public static class reducer extends Reducer<Text, Text, NullWritable, Text>{
      
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text v =new Text();
			//用户id，行为分类，用户访问次数，分类总访问次数，标准值（用于展示）
			int n=0;//用户访问次数
			int pv=0;//分类总访问次数
			Map<String, Integer> map = new HashMap();
			for (Text val:values){
				String[] str = val.toString().split(MProperties.getValue("outsplit"));
				//value：用户ID，访问次数
				pv+=Integer.parseInt(str[1]);
				//同一个用户访问不同的网页但是这些网页是同一个分类，需要把这些值相加
				if(map.containsKey(str[0])){
					int num=map.get(str[0]);//原来已经存在的用户访问次数
		    		map.put(str[0],  num+ Integer.parseInt(str[1]) );
		    	}else{
		    		
		    		map.put(str[0], Integer.parseInt(str[1]));
		    		//统计一共有多少用户
		    		n = n +1;
		    	}
			}
	//z-score标准差计算（反映数据集的离散程度，公式：（原数据-均值）/标准差）
	//均值=行为分类总次数/行为分类总人数
	//标准差:等同于数学中的标准差求解
		    double sum = 0;
		    //均值
		    double avg = pv/n ;
		    //每个key的信息  用户id ：一类网页访问次数
		    for(String s : map.keySet()){
		    	//sum=数据-均值后的平方累加和，
		    	sum = sum + Math.pow(map.get(s)-avg,2);
		    }
		    //标准差(方差开根号，方差=sum/数据条数)
		    double fc = Math.sqrt(sum/n);	    
		    //循环用户列表 
		    for(String s : map.keySet()){
		    	//z-score 标准化,（原数据-均值）/标准差
		    	/*标准分数（standard score）也叫z分数（z-score）,是一个分数与平均数的差再除以标准差的过程。用公式表示为：
		    	z=(x-μ)/σ。其中x为某一具体分数，
		    	μ为平均数，σ为标准差。
		    	Z值的量代表着原始分数和母体平均值之间的距离，是以标准差为单位计算。在原始分数低于平均值时Z则为负数，反之则为正数。*/
		    	double b = (map.get(s)-avg)/fc;						
		     //用户ID，分类ID，用户分类pv，分类总体pv，标准值
	    	  v.set(s + MProperties.getValue("outsplit")
	    			+ key.toString() + MProperties.getValue("outsplit")
	    			+ map.get(s) + MProperties.getValue("outsplit")
	    			+ pv + MProperties.getValue("outsplit") 
	    			+  b); 
	    	  context.write(NullWritable.get(), v);
			}
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Personas.class);
		job.setMapperClass(mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(reducer.class);
		//输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);						
		job.addCacheFile(new URI("hdfs://192.168.245.20:9000/telecom/config/t_dx_basic_classify_link.txt"));			
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data3"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data9");			
		FileOutputFormat.setOutputPath(job, out);
		FileSystem fileSystem = out.getFileSystem(conf);       
		//getFileSystem()函数功能  Return the FileSystem that owns this Path.   
		    if (fileSystem.exists(out)) {  
		        fileSystem.delete(out,true);  
		    }  
		boolean res=job.waitForCompletion(true);
	}

}
