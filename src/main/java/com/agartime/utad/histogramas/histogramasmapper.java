package histogramas;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class histogramasmapper  extends Mapper<Text, Image, Text, LongArrayWritable> {
	private final static LongWritable one = new LongWritable(1);

	@Override
	public void map(Text key, Image value, Context context)
            throws IOException, InterruptedException {
    
    // Convert to gray scale image
    IplImage im1 = value.getImage();
    IplImage im2 = cvCreateImage(cvSize(im1.width,im1.height), IPL_DEPTH_8U, 1);
    cvCvtColor(im1, im2, CV_BGR2GRAY);
    
    // Initialize histogram array
    LongWritable [] histogram = new LongWritable[256];
    for(int i = 0; i < histogram.length; i++){
            histogram[i] = new LongWritable();
    }
    
    // Compute histogram
    ByteBuffer buffer = im2.getByteBuffer();
    while(buffer.hasRemaining()){
            int val = buffer.get() + 128;
            histogram[val].set(histogram[val].get() + 1);
    }
    
    context.write(key, new LongArrayWritable(histogram));
}
}



