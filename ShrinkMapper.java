import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ShrinkMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	static final double INF = Double.MAX_VALUE;


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String input = value.toString();
		StringBuilder result = new StringBuilder();
		String[] record = input.split("\t")[1].split(" ");
		SvmData data=new SvmData(record);
		double Gmax1 = -INF; // max { -y_i * grad(f)_i | i in I_up(\alpha) }
		double Gmax2 = -INF; // max { y_i * grad(f)_i | i in I_low(\alpha) }
		if (data.y == +1) {
			if (!SvmUtil.is_upper_bound(data)) {
				if (-data.G >= Gmax1)
					Gmax1 = -data.G;
			}
			if (!SvmUtil.is_lower_bound(data)) {
				if (data.G >= Gmax2)
					Gmax2 = data.G;
			}
		} else {
			if (!SvmUtil.is_upper_bound(data)) {
				if (-data.G >= Gmax2)
					Gmax2 = -data.G;
			}
			if (!SvmUtil.is_lower_bound(data)) {
				if (data.G >= Gmax1)
					Gmax1 = data.G;
			}
		}
		result.append(Gmax1 + " " + Gmax2);
		context.write(new LongWritable(1), new Text(result.toString()));

	}
}
