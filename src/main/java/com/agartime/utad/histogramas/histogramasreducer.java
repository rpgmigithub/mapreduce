package histogramas;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class histogramasreducer extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
    
	@Override
	public void reduce(Text key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException {

		// Initialize histogram array
		LongWritable [] histogram = new LongWritable[256];
		for(int i = 0; i < histogram.length; i++){
			histogram[i] = new LongWritable();
		}

		// Sum the parts
		Iterator<LongArrayWritable> it = values.iterator();
		while (it.hasNext()) {
			LongWritable[] part = (LongWritable[]) it.next().toArray();
			for(int i = 0; i < histogram.length; i++){
                histogram[i].set(histogram[i].get() + part[i].get());
			}
		}

		context.write(key, new LongArrayWritable(histogram));
	}
}
