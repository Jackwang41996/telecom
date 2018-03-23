package CountMatch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Tools.MProperties;

public class UserDomain {
	//523046669407,005460000000000000,0,0,http://alog.umeng.com/app_logs
    //用户id，行为id 是否产品标识 预购类型   网址
  public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 /**
  * k,用户id+行为id(0,6)
  * v,1
  */
	  Text k=new Text();
	 IntWritable v= new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] line=value.toString().split(MProperties.getValue("outsplit"));
		k.set(line[0]+","+line[1].substring(0,6)+"000000000000");
		context.write(k,v);
	}
	  
  }
	public static class reducer extends Reducer<Text, IntWritable,NullWritable, Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable x:values){
				sum+=x.get();
			}
			v.set(key.toString()+ MProperties.getValue("outsplit") + sum);
          context.write(NullWritable.get(), v);
		}
		
	}	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(UserDomain.class);		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data2"));
		//Path out=new Path("hdfs://192.168.245.20:9000/telecom/data3");
		Path out=new Path(args[1]);
		FileSystem fileSystem = out.getFileSystem(conf);       
		//getFileSystem()函数功能  Return the FileSystem that owns this Path.   
		    if (fileSystem.exists(out)) {  
		        fileSystem.delete(out,true);  
		    }  
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);

	}

}
