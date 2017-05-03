package topProductsMonths;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductWritable implements WritableComparable<ProductWritable> {

	private Text id;
	private double score;

	public ProductWritable(){
	}

	public ProductWritable(Text id, Double score) {
		this.id = id;
		this.score = score;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = new Text(in.readUTF());
		score = new Double(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.id.toString());
		out.writeUTF(String.valueOf(this.score));
	}

	public Text getId() {
		return id;
	}

	public void setId(Text id) {
		this.id = id;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return this.id.toString() + " " + String.valueOf(this.score);
	}

	@Override
	public int hashCode() {
		return this.id.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ProductWritable) {
			ProductWritable product = (ProductWritable) o;
			return this.id.equals(product.getId());
		}
		return false;
	}

	@Override
	public int compareTo(ProductWritable o) {
		Double result = o.getScore() - this.score;
		int i = result.intValue();
		return i;
	}
}