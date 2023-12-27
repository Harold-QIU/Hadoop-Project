package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

public class StockReducer extends Reducer<Text, Text, Text, Text> {
    protected String[] order;
    protected ArrayList<String[]> cancelList = new ArrayList<>();
    protected String v;
    /**
     * 用于存储MarketOrder的价格
     */
    protected HashSet<String> priceSet = new HashSet<>();
    /**
     * 原时间格式
     */
    protected SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    /**
     * 转换后的时间格式
     */
    protected SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 清空order, cancelList, priceSet
        order = null;
        cancelList.clear();
        priceSet.clear();

        // 判断传入value是order还是trade
        for (Text value : values) {
            String[] split = value.toString().split(",");
            if (!split[4].equals("b") && !split[4].equals("s")) {
                // v: Price, OrderQty, TransactTime, Side, OrderType
                order = split;
            } else {
                // v: Price, TradeQty, ExecType, tradeTime, b or s
                if (split[2].equals("F")) {
                    priceSet.add(split[0]);
                } else {
                    cancelList.add(split);
                }
            }
        }

        if (order != null) {
            // 处理Order
            switch (order[4]) {
                case "U": // 处理SpecOrder
                    v = tConvert(order[2]) + ",," + order[1] + "," + order[3] + "," + order[4] + "," + key + ",," + 2;
                    context.write(null, new Text(v));
                    break;
                case "2": // 处理LimitedOrder
                    // 将价格转换为double，再转换为String
                    double price = Double.parseDouble(order[0]);
                    v = tConvert(order[2]) + "," + price + "," + order[1] + "," + order[3] + "," + order[4] + "," + key + ",," + 2;
                    context.write(null, new Text(v));
                    break;
                case "1": // 处理MarketOrder
                    v = tConvert(order[2]) + ",," + order[1] + "," + order[3] + "," + order[4] + "," + key + "," + priceSet.size() + "," + 2;
                    context.write(null, new Text(v));
                    break;
            }
        }


        // 判断cancelList是否为空
        if (!cancelList.isEmpty()) {
            // 处理Cancel
            for (String[] trade : cancelList) {
                if (order != null) {
                    v = tConvert(trade[3]) + ",," + trade[1] + "," + order[3] + "," + order[4] + "," + key + ",," + 1;
                    context.write(null, new Text(v));
                } else {
                    if (trade[4].equals("b")) {
                        v = tConvert(trade[3]) + ",," + trade[1] + ",1,0," + key + ",," + 1;
                        context.write(null, new Text(v));
                    } else {
                        v = tConvert(trade[3]) + ",," + trade[1] + ",2,0," + key + ",," + 1;
                        context.write(null, new Text(v));
                    }
                }

            }
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
