package freewheel.rpt.mr;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WalReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("\n");
        for(Text text : values){
            stringBuffer.append("\t\t");
            stringBuffer.append(text.toString());
            stringBuffer.append("\n");
        }
        context.write(key,new Text(stringBuffer.toString()));
    }

}
