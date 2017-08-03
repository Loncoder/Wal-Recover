package freewheel.rpt.mr;

import freewheel.rpt.Pair;
import freewheel.rpt.ParserWal;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *  Minimum((byte)0),
 Put((byte)4),
 Delete((byte)8),
 DeleteFamilyVersion((byte)10),
 DeleteColumn((byte)12),
 DeleteFamily((byte)14),
 Maximum((byte)-1);
 * */

public class WalMapper extends Mapper<WALKey,WALEdit,Text,Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(WALKey key, WALEdit edit, Context context) throws IOException, InterruptedException {
        System.out.println("key" + key);
        System.out.println("value" + edit);
        if(key!=null && edit!=null) {
            long writeTime = key.getWriteTime();
            String tbName = Bytes.toString(key.getTablename().getName());

            for (Cell cell : edit.getCells()) {
                Pair<String,String> pair = ParserWal.parseWalCell(cell);
                if(pair!=null)
                context.write(new Text(tbName+"|"+pair.getFirst()+"|"+writeTime),new Text(pair.getSecond()));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
