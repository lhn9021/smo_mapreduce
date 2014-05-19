
public class SvmData {

	int index;
	float[] values;
	boolean active;
	double G;
	double G_bar;
	double kernel_value;
	double y;
	double alpha;
	byte alpha_status;
	int num_feature;
	
	public SvmData(String[] data){
		int length=data.length;
		this.index=Integer.parseInt(data[0]);
		this.num_feature=(data.length-10)/2;
		this.values=new float[num_feature];
		for(int i=0;i<num_feature;i++){
			values[i]=Float.parseFloat(data[2+2*i]);
		}
		this.active=Boolean.parseBoolean(data[length-7]);
		this.G=Double.parseDouble(data[length-6]);
		this.G_bar=Double.parseDouble(data[length-5]);
		this.kernel_value=Double.parseDouble(data[length-4]);
		this.y=Double.parseDouble(data[length-3]);
		this.alpha=Double.parseDouble(data[length-2]);
		this.alpha_status=Byte.parseByte(data[length-1]);
	}
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(Integer.toString(index)).append(" ");
		for(int i=1;i<=num_feature;i++){
			sb.append(Integer.toString(i)).append(" ").append(Float.toString(values[i-1])).append(" ");
		}
		sb.append(Boolean.toString(active)).append(" ");
		sb.append(Double.toString(G)).append(" ");
		sb.append(Double.toString(G_bar)).append(" ");
		sb.append(Double.toString(kernel_value)).append(" ");
		sb.append(Double.toString(y)).append(" ");
		sb.append(Double.toString(alpha)).append(" ");
		sb.append(Byte.toString(alpha_status));
		
		return sb.toString();
		
	}
}
