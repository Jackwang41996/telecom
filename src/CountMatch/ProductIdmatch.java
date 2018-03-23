package CountMatch;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Tools.MProperties;

public class ProductIdmatch {
	//523046669407,005460000000000000,0,0,http://alog.umeng.com/app_logs
	 //用户id，行为id 是否产品标识 预购类型   网址
   public  static 	class mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	   Text k=new Text();
	   Map<String,String> promap=new HashMap<String,String>();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] line=value.toString().split(MProperties.getValue("outsplit"));
		if(promap.containsKey(line[1])){
			StringBuffer sb = new StringBuffer();
			sb.append(promap.get(line[1])).append("|").append(line[0]);
	       k.set(sb.toString());	
	       context.write(k, NullWritable.get());
	}		
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		BufferedReader br=new BufferedReader(new FileReader("t_dx_product_msg_addr.txt"));
		String line=null;
		//关联产品地址库（t_dx_product_msg_addr.txt）标识用户行为匹配中的产品地址。
		//029756000001000010|car|car000001|DS-DS 6-无限制|DS|16.39-30.19万|DS 6|欧系其他|无限制
		//行为id 产品类型 产品id 产品名称 产品特征值1 产品特征值2 产品特征值 3 产品特征值
		
		while((line=br.readLine())!=null){
			String[] datas=line.split(MProperties.getValue("msgaddrspilt"));
			promap.put(datas[0],line);			
		}
		br.close();
	}
	   
   }
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(ProductIdmatch.class);		
		job.setMapperClass(mapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);			
		job.setNumReduceTasks(0);			
		job.addCacheFile(new URI("hdfs://192.168.245.20:9000/telecom/config/t_dx_product_msg_addr.txt"));			
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data2"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data5");			
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}
}
