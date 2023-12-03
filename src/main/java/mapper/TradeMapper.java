package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] fields = input.split("\t");

        // 判断证券代码受否为000001，时间是否合法
        String securityID = fields[8];
        long transactTime = Long.parseLong(fields[10]);
        if (!validate(securityID, transactTime)) return;

        // k1: BidApplSeqNum, k2: OfferApplSeqNum
        Text k1 = new Text(fields[10]);
        Text k2 = new Text(fields[11]);

        // v: Price, TradeQty, ExecType, tradeTime
        Text v = new Text(fields[12] + "," + fields[13] + "," + fields[14] + "," + fields[15]);

        context.write(k1, v);
        context.write(k2, v);
    }

    /**
     * @param securityID 证券代码
     * @param transactTime 委托时间
     * @return 证券代发和委托时间是否合法
     */
    protected boolean validate(String securityID, long transactTime) {
        return securityID.equals("000001") && transactTime >= 20190102093000000L && (transactTime <= 20190102113100000L || transactTime >= 20190102133000000L) && transactTime <= 20190102145700000L;
    }
}
