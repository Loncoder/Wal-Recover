package freewheel.rpt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;

public class WalClient {

    public static void main(String[] args) {

        String head = "hdfs://127.0.0.1:9000";
        String path = "/wal-test/input/";
        printWalDetails(new Path(head+path));
    }

    public static void readDetails(WAL.Reader reader) throws IOException {
        WAL.Entry entry;

        while ((entry=reader.next())!=null){

            //WALKey key = entry.getKey();
            //long writeTime = key.getWriteTime();
            //String tbName = Bytes.toString(key.getTablename().getName());
            //System.out.println("writeTime " + writeTime);
            //System.out.println("tbName" + tbName);

            WALEdit edit = entry.getEdit();
            for(Cell cell : edit.getCells()){
                Pair<String,String> pair = ParserWal.parseWalCell(cell);
                System.out.println(pair);
            }

        }
    }

    public static void printWalDetails(Path walsPath){
        Configuration conf=new Configuration();
        //Configuration conf = HBaseConfiguration.create();
        FileSystem fs = null;
        FileStatus[] fsFiles = null;
        System.out.println();
        try {
            //fs = FileSystem.get(conf);

            fs=FileSystem.get(URI.create(walsPath.toString()),conf);
            //fs.create(walsPath);
            fsFiles = fs.listStatus(walsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            System.out.println(fs.isFile(walsPath));
            System.out.println(fs.isDirectory(walsPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(walsPath.toString());
        System.out.println("get files " + fsFiles);

        for(FileStatus walfs : fsFiles){

            System.out.println(walfs.getPath().getName()+"\t"+ new Date(walfs.getAccessTime()));

            if(walfs.isFile()){
                try {
                    WAL.Reader log = WALFactory.createReader(fs,walfs.getPath(),conf);
                    readDetails(log);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
