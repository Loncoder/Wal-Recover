package freewheel.rpt;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Properties;

public class ProLoader {
    public static Properties loadPro(String path){
        Properties properties = new Properties();

        try {
            properties.load(ProLoader.class.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static Configuration loadConf(String path){
        Properties properties = loadPro(path);
        Configuration conf  = new Configuration();
        for (Object oKey : properties.keySet()){
            conf.set((String)oKey,properties.getProperty((String)oKey));
        }
        return conf;
    }
}
