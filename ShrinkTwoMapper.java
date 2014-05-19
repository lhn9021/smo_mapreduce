import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ShrinkTwoMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	private MultipleOutputs<LongWritable, Text> mos;

	private boolean be_shrunk(SvmData data, double Gmax1, double Gmax2) {
		if (SvmUtil.is_upper_bound(data)) {
			if (data.y == +1)
				return (-data.G > Gmax1);
			else
				return (-data.G > Gmax2);
		} else if (SvmUtil.is_lower_bound(data)) {
			if (data.y == +1)
				return (data.G > Gmax2);
			else
				return (data.G > Gmax1);
		} else
			return (false);
	}

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<LongWritable, Text>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		double Gmax1=Double.parseDouble(conf.get("Gmax1"));
		double Gmax2=Double.parseDouble(conf.get("Gmax2"));
		int iter=Integer.parseInt(conf.get("iter"));
		String input = value.toString();
		String[] split=input.split("\t");
		String[] record = split[1].split(" ");
		SvmData data = new SvmData(record);
		if (be_shrunk(data, Gmax1, Gmax2)) {
			mos.write("MOSText", new LongWritable(data.index),new Text(data.toString()),"active_"+iter+"/");
		} else {
			data.active=false;
			mos.write("MOSText", new LongWritable(data.index),new Text(data.toString()),"inactive_"+iter+"/");
		}
	}
}
