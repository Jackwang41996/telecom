package DataClear;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author Administrator
 *args[0]源路径"hdfs://192.168.245.20:9000/telecom/data/data"
 *args[1]目的路径"hdfs://192.168.245.20:9000/telecom/data1/"
 */
public class FirstMapper {
 static class Mapper1 extends Mapper<LongWritable, Text, NullWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String data[]=line.split("");
		String data1=null;
		String data14=null;
		String end="jpg|png|bmp|jpeg|tif|gif|psd|ico|pdf|css|tmp|js|gz|rar|gzip|zip|txt|csv|xlsx|xls|webp|src";
		//http://123.151.176.115:8080/		
		if((data[14].startsWith("http://") || data[14].startsWith("https://"))){
			if(data[14].length()>7 || data[14].length()>8){
			if(!(end.contains(data[14].substring(data[14].length()-3, data[14].length())))){
		      data14=data[14].split("/", -1)[2].split(":")[0];
		      data1=data[1]+","+data14+","+data[14];		 
		      context.write( NullWritable.get(),new Text(data1));	
			 }
		}else {
			if(data[14].length()>7|| data[14].length()>8 ){
				if(!(end.contains(data[14].substring(data[14].length()-3, data[14].length())))){
			data[14]="http://"+data[14];
			data14=data[14].split("/", -1)[2];
			 data1=data[1]+","+data14+","+data[14];		 
			context.write(NullWritable.get(),new Text(data1));	
			}
			}
		}
		}
	}
	}
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(FirstMapper.class);		
		job.setMapperClass(Mapper1.class);	
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);				
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data/data"));
		//Path out=new Path(args[1]);	
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data1");
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}

}