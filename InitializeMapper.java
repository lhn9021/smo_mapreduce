import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InitializeMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	static double gamma = 0;
	static double coef0 = 0;
	static int degree = 0;
	static int kernel_type=2;
	static final byte LOWER_BOUND = 0;
	static final byte UPPER_BOUND = 1;
	static final byte FREE = 2;
	static double Cp=1,Cn=1;

	static double kernel_function(int kernel_type, float[] i, float[] j) {
		switch (kernel_type) {
		case svm_parameter.LINEAR:
			return SvmUtil.dot(i, j);
		case svm_parameter.POLY:
			return SvmUtil.powi(gamma * SvmUtil.dot(i, j) + coef0, degree);
		case svm_parameter.RBF:
			return Math.exp(-gamma * (SvmUtil.square(i) + SvmUtil.square(j) - 2 * SvmUtil.dot(i, j)));
		case svm_parameter.SIGMOID:
			return Math.tanh(gamma * SvmUtil.dot(i, j) + coef0);
		case svm_parameter.PRECOMPUTED:
			return i[(int) (j[0])];
		default:
			return 0; // java
		}
	}

	

	void update_alpha_status(double alpha, int alpha_status,double y) {
		if (alpha >= y)
			alpha_status = UPPER_BOUND;
		else if (alpha <= 0)
			alpha_status = LOWER_BOUND;
		else
			alpha_status = FREE;
	}

	double get_C(double y) {
		return (y > 0) ? Cp : Cn;
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration config=context.getConfiguration();
		coef0=Double.parseDouble(config.get("coef0"));
		kernel_type=Integer.parseInt(config.get("kernel_type"));
		degree=Integer.parseInt(config.get("degree"));
		gamma=Double.parseDouble(config.get("gamma"));
		Cp=Double.parseDouble(config.get("C"));
		Cn=Double.parseDouble(config.get("C"));
		String input = value.toString();
		StringBuffer result = new StringBuffer();
		StringTokenizer st = new StringTokenizer(input, " \t\n\r\f:");
		float y = Float.parseFloat(st.nextToken());
		int m = st.countTokens() / 2;
		int index;
		float[] values = new float[m];
		result.append(key.toString());
		for (int j = 0; j < m; j++) {
			index = Integer.parseInt(st.nextToken());
			values[j] = Float.parseFloat(st.nextToken());
			result.append(" " + index + " " + values[j]);
		}
		result.append(" true -1.0 0.0 ");//Active? G G_bar 
		result.append(kernel_function(kernel_type, values, values));//kernel value
		result.append(" " + y);
		result.append(" 0.0 ");//alpha
		int alpha_status=0;
		update_alpha_status(0,alpha_status,y);
		result.append(alpha_status);//alpha_status
		
		context.write(key, new Text(result.toString()));

	}
}
