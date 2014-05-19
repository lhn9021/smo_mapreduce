
public class SvmUtil {
	static double powi(double base, int times) {
		double tmp = base, ret = 1.0;

		for (int t = times; t > 0; t /= 2) {
			if (t % 2 == 1)
				ret *= tmp;
			tmp = tmp * tmp;
		}
		return ret;
	}

	static double square(float[] x) {
		double sum = 0;
		int length = x.length;
		for (int i = 0; i < length; i++) {
			sum += x[i] * x[i];
		}
		return sum;
	}

	static double dot(float[] x, float[] y) {
		double sum = 0;
		int xlen = x.length;
		int ylen = y.length;
		int i = 0;
		int j = 0;
		while (i < xlen && j < ylen) {
			if (i == j)
				sum += x[i++] * y[j++];
			else {
				if (i > j)
					++j;
				else
					++i;
			}
		}
		return sum;
	}
	static final byte LOWER_BOUND = 0;
	static final byte UPPER_BOUND = 1;

	static boolean is_upper_bound(SvmData data) {
		return data.alpha_status == UPPER_BOUND;
	}

	static boolean is_lower_bound(SvmData data) {
		return data.alpha_status == LOWER_BOUND;
	}
}
