package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k1;
    Text k2;
    String str;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] fields = input.split("\t");

        // 判断证券代码受否为000001，时间是否合法
        String securityID = fields[8];
        if (!validate(securityID, fields[15])) return;

        // v: Price, TradeQty, ExecType, tradeTime
        str = fields[12] + "," + fields[13] + "," + fields[14] + "," + fields[15];

        // k1: BidApplSeqNum + buy flag
        if (!fields[10].equals("0")) {
            k1 = new Text(fields[10]);
            context.write(k1, new Text(str + ",b"));
        }

        // k2: OfferApplSeqNum + sale flag
        if (!fields[11].equals("0")) {
            k2 = new Text(fields[11]);
            context.write(k2, new Text(str + ",s"));
        }

    }

    /**
     * @param securityID 证券代码
     * @param transactTime 委托时间
     * @return 证券代发和委托时间是否合法
     */
    protected boolean validate(String securityID, String transactTime) {
        return securityID.equals("000001") && transactTime.compareTo("20190102093000000") >= 0 && (transactTime.compareTo("20190102113100000") < 0 || transactTime.compareTo("20190102130000000") >= 0) && transactTime.compareTo("20190102145700000") < 0;
    }
}
