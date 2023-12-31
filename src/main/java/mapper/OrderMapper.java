package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] fields = input.split("\t");

        // 判断证券代码，时间是否合法
        String securityID = fields[8];
        if (!validate(securityID, fields[12])) return;

        // k: ApplSeqNum
        Text k = new Text(fields[7]);

        // v: Price, OrderQty, TransactTime, Side, OrderType
        Text v = new Text(fields[10] + "," + fields[11] + "," + fields[12] + "," + fields[13] + "," + fields[14]);

        context.write(k, v);
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
