package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

public class StockReducer extends Reducer<Text, Text, Text, Text> {
    protected String[] order;
    protected String[] trade;
    protected String v;
    /**
     * 用于存储MarketOrder的价格
     */
    protected HashSet<String> priceSet = new HashSet<>();
    /**
     * 原时间格式
     */
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    /**
     * 转换后的时间格式
     */
    SimpleDateFormat outputFormat = new SimpleDateFormat("M/d/yyyy h:mm:ss a");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 判断传入value是order还是trade
        for (Text value: values) {
            String[] split = value.toString().split(",");
            if (split.length == 5) {
                // v: Price, OrderQty, TransactTime, Side, OrderType
                order = split;
            } else {
                // v: Price, TradeQty, ExecType, tradeTime
                trade = split;
                if (trade[2].equals("F")) {
                    priceSet.add(trade[0]);
                }
            }
        }

        if (order == null) {
            return;
        }

        // 处理SpecOrder
        if (order[4].equals("U")) {
            v = tConvert(order[2]) + ",," + order[1] +  "," + order[3] + "," + order[4] + "," + key + ",," + 2;
            context.write(null, new Text(v));
        }

        // 处理LimitedOrder
        if (order[4].equals("2")) {
            v = tConvert(order[2]) + "," + order[0] + "," + order[1] + "," + order[3] + "," + order[4] + "," + key + ",," + 2;
            context.write(null, new Text(v));
        }

        // 判断trade是否为空
        if (trade == null) {
            return;
        }

        // 处理Cancel
        if (trade[2].equals("4")) {
            v = tConvert(trade[3]) + ",," + trade[1] + "," +  order[3] + "," + order[4] + "," + key + ",," + 1;
            context.write(null, new Text(v));
        }

        // 处理MarketOrder
        if (trade[2].equals("F")) {
            v = tConvert(order[2]) + ",," + order[1] + "," + order[3] + "," + order[4] + "," + key + "," + priceSet.size() + "," + 2;
            context.write(null, new Text(v));
        }

    }

    /**
     * @param t 转换前的时间
     * @return 转换后的时间
     */
    protected String tConvert(String t) {
        Date date;
        try {
            date = inputFormat.parse(t);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return outputFormat.format(date);
    }
}
