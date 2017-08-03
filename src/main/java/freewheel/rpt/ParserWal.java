package freewheel.rpt;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public  class ParserWal {
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
    public static Pair<String,String> parseWalCell(Cell cell){
        byte type = cell.getTypeByte();
        boolean isDelete = false;
        if(type==8 || type==10 || type==12 || type == 14){
            isDelete=true;
        }
        byte [] browKey = cell.getRow();
        String rowKey = Bytes.toString(browKey);
        String cellDescription = Bytes.toString(cell.getFamily()) + ":" + Bytes.toString(cell.getQualifier());;
        String value =  Bytes.toString(cell.getValue());

        boolean isMeta = (browKey[0]==0&&browKey.length==1)?true:false;
        if(!isMeta) {
            String info = cellDescription + " | " + value + " | " + (isDelete ? "Detete" : "Insert");
            Pair<String, String> pair = new Pair<>(rowKey, info);
            return pair;
        }else{
            return new Pair<>("meta","meta");
        }
    }

}
