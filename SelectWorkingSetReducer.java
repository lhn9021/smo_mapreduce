import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SelectWorkingSetReducer extends
		Reducer<LongWritable, Text, DoubleWritable, Text> {
	static final double INF = Double.MAX_VALUE;

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String result = null;
		double Gmax = -INF;
		int Gmax_idx = -1;
		for (Text text : values) {
			String[] split=text.toString().split("\t");
			String[] record = split[1].split(" ");
			SvmData data=new SvmData(record);
			if (data.y == +1) {
				if (!SvmUtil.is_upper_bound(data))
					if (-data.G >= Gmax) {
						Gmax = -data.G;
						result = split[1];
						Gmax_idx= data.index;
					}
			} else {
				if (!SvmUtil.is_lower_bound(data))
					if (data.G >= Gmax) {
						Gmax = data.G;
						result = split[1];
						Gmax_idx= data.index;
					}
			}
		}
		context.write(new DoubleWritable(Gmax), new Text(result));
	}

}
