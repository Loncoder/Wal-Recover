package freewheel.rpt.mr;

import freewheel.rpt.ProLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*if(args.length!=1){
            System.out.println("wal.properties");
            return;
        }*/

        Logger log = Logger.getLogger(Main.class);
        String path = "wal.properties";
        Configuration conf = ProLoader.loadConf(path);



        Job job = new Job(conf, "wal parser");
        job.setJarByClass(Main.class);
        job.setMapperClass(WalMapper.class);
        job.setReducerClass(WalReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String fs = conf.get("hdfsprefix");
        String inputDir = conf.get("in-dir");
        String outputDir = conf.get("out-dir");
        log.info("fs: "+fs);
        log.info("in-dir: " + inputDir);
        log.info("out-dir: " + outputDir);
        job.setInputFormatClass(WALInputFormat.class);



        FileInputFormat.setInputPaths(job,fs+inputDir);


        FileOutputFormat.setOutputPath(job, new Path(fs+outputDir+"/out"+date()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String date(){
        SimpleDateFormat myFmt2=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        Date now=new Date();
        return myFmt2.format(now);
    }
}
