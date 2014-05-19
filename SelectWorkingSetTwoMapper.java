import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SelectWorkingSetTwoMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO read from configuration
		Configuration config=context.getConfiguration();
		String i_data=config.get("data");
		double Gmax=Double.parseDouble(config.get("Gmax"));
		String[] record=i_data.split(" ");
		SvmData i=new SvmData(record);
		String[] split = value.toString().split("\t");
		String[] map_record=split[1].split(" ");
		SvmData data=new SvmData(map_record);
		if (data.y == +1) {
			if (!SvmUtil.is_lower_bound(data)) {
				double grad_diff = Gmax + data.G;
				if (grad_diff > 0) {
					double obj_diff;
					double quad_coef = i.kernel_value + data.kernel_value - 2.0
							* i.y * SvmUtil.dot(i.values, data.values);
					if (quad_coef > 0)
						obj_diff = -(grad_diff * grad_diff) / quad_coef;
					else
						obj_diff = -(grad_diff * grad_diff) / 1e-12;
					context.write(new LongWritable(1), new Text(new String(split[1] + " "
							+ obj_diff)));
				}
			}
		} else {
			if (!SvmUtil.is_upper_bound(data)) {
				double grad_diff = Gmax - data.G;
				if (grad_diff > 0) {
					double obj_diff;
					double quad_coef = i.kernel_value + data.kernel_value + 2.0
							* i.y * SvmUtil.dot(i.values, data.values);
					if (quad_coef > 0)
						obj_diff = -(grad_diff * grad_diff) / quad_coef;
					else
						obj_diff = -(grad_diff * grad_diff) / 1e-12;
					context.write(new LongWritable(1), new Text(new String(obj_diff + "\t"
							+ split[1])));
				}
			}
		}
	}
}
