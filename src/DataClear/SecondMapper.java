package DataClear;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondMapper {
	
	static class mapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		 HashMap<String,String> promap=null;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] datas=line.split(",");
			String url=datas[1];
			//513047538993,muhe.ymby168.com,http://muhe.ymby168.com:8066/ct.php?hash=dcbb9e05db
			String proinfo=promap.get(url);
			//System.out.println(proinfo);
			if(proinfo !=null){
			String result=line+","+proinfo;
			context.write(NullWritable.get(),new Text(result) );}			
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br=new BufferedReader(new FileReader("t_dx_basic_msg_addr.txt"));
			String line=null;
			promap=new HashMap<String,String>();
//			029195000008000000|mop.com|tt.mop.com|2|0|0
//			行为id 一级域名 匹配地址 匹配级别 是否产品标识 预购类型
			while((line=br.readLine())!=null){
				String[] datas=line.split("\\|");
				String str=datas[0]+","+datas[4]+","+datas[5];
				promap.put(datas[2],str);
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
	job.addCacheFile(new URI("hdfs://192.168.245.20:9000/telecom/config/t_dx_basic_msg_addr.txt"));			
	FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data1"));
	Path out=new Path("hdfs://192.168.245.20:9000/telecom/data2");			
	FileOutputFormat.setOutputPath(job, out);
	boolean res=job.waitForCompletion(true);
}
}
