package DataClear;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import DataClear.FirstMapper.Mapper1;
import Tools.MProperties;

public class DataClear {
	public static class mapper extends Mapper<LongWritable, Text, NullWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Text text=new Text();
			String line=value.toString();
			String[] str=line.split(MProperties.getValue("srcsplit"));
			if(str.length==15
					&& !"http://".equals(str[14])
					&& !"https://".equals(str[14])
					&& !"".equals(str[14])
					&& !(str[14].toLowerCase()).matches(MProperties.getValue("end"))){
				StringBuffer sb = new StringBuffer();
				if (!str[14].startsWith("http://") && !str[14].startsWith("https://")) {
					str[14] = "http://" + str[14];
				}
				String domain = str[14].split("/", -1)[2];				
		        if (domain.indexOf(":") >= 0) {
					domain = domain.split("\\:", -1)[0];
				}
		        if(domain.matches(MProperties.getValue("ip"))){
						  domain=domain;
					}
		        Pattern pattern = Pattern.compile(MProperties.getValue("domin"));
		        Matcher matcher = pattern.matcher(domain);
		        if(matcher.find()){
		        	 domain=matcher.group(1);
		        } 
				sb.append(str[1]).append(MProperties.getValue("outsplit"))
						.append(domain).append(MProperties.getValue("outsplit"))
						.append(str[14]);
				text = new Text(sb.toString());
				context.write(NullWritable.get(), text);
			}			
		}		
	}	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	  Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(DataClear.class);		
		job.setMapperClass(mapper.class);	
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data/data"));
		Path out=new Path(args[1]);	
		//Path out=new Path("hdfs://192.168.245.20:9000/telecom/data1");
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
}
}
