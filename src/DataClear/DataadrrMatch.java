package DataClear;
/*
 * @对数据进行清洗
 * 523046669407,002974000000000000,1,0,0,http://nav.gionee.com/lockimage/logUpload.do?f=1&lan=zh&mcc=460&v=1.2.0.ck&t=1498877023538&s=C6B10F952FB1500A738AABEC4936E82B
 * 
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Tools.MComparator;
import Tools.MProperties;
public class DataadrrMatch {
      public static class mapper extends Mapper<LongWritable, Text, NullWritable, Text>{
    	  private Map<String, TreeMap<String, String>> promap = new HashMap<String, TreeMap<String, String>>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] datas=line.split(MProperties.getValue("outsplit"));
			String url=datas[1];
			//513047538993,ymby168.com,http://muhe.ymby168.com:8066/ct.php?hash=dcbb9e05db
			if(promap.containsKey(url)){
				TreeMap<String, String> proinfo=promap.get(url);
				Iterator iterator = proinfo.keySet().iterator();
			  while (iterator.hasNext()) {
				String[] str = ((String) iterator.next()).split(",");
				if (datas[2].indexOf(str[1]) >= 0) {					
					StringBuffer sb = new StringBuffer();
					sb.append(datas[0]).append(MProperties.getValue("outsplit"))					
					.append(str[0]).append(MProperties.getValue("outsplit"))
					//.append(str[2]).append(MProperties.getValue("outsplit"))					
					.append(str[3]).append(MProperties.getValue("outsplit"))					
					.append(str[4]).append(MProperties.getValue("outsplit"))
					.append(datas[2]);
					Text text = new Text( sb.toString() );
					context.write(NullWritable.get(), text);
					return ;
				}
			}			
		}
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br=new BufferedReader(new FileReader("t_dx_basic_msg_addr.txt"));
			String line=null;
			
//			029195000008000000|mop.com|tt.mop.com|2|0|0
//			行为id 一级域名 匹配地址 匹配级别 是否产品标识 预购类型
			while((line=br.readLine())!=null){
				String[] datas=line.split(MProperties.getValue("msgaddrspilt"));
				//清洗数据所以加&& Integer.parseInt(datas[4])!=0 && Integer.parseInt(datas[5])!=0
				if(datas.length==6 ){			
				if(promap.containsKey(datas[1])){
					promap.get(datas[1]).put(datas[0]+","+datas[2]+","+datas[3]+","+datas[4]+","+datas[5], "");
				}else{
					TreeMap<String, String> treemap = new TreeMap<String, String>( new MComparator());
					treemap.put(datas[0]+","+datas[2]+","+datas[3]+","+datas[4]+","+datas[5], "");
					promap.put(datas[1], treemap);
				}}				
			}
			br.close();
		}   
      
      }	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(SecondMapper.class);		
		job.setMapperClass(mapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);			
		job.setNumReduceTasks(0);	
		job.addCacheFile(new URI(args[0]));
		//job.addCacheFile(new URI("hdfs://192.168.245.20:9000/telecom/config/t_dx_basic_msg_addr.txt"));			
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data1"));
		//Path out=new Path("hdfs://192.168.245.20:9000/telecom/data2");
		Path out=new Path(args[1]);
		FileOutputFormat.setOutputPath(job, out);
		FileSystem fileSystem = out.getFileSystem(conf);       
		//getFileSystem()函数功能  Return the FileSystem that owns this Path.   
		    if (fileSystem.exists(out)) {  
		        fileSystem.delete(out,true);  
		    }  
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}

}
