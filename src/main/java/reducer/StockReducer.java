package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class StockReducer extends Reducer<Text, Text, Text, Text> {
    protected String[] order;
    protected String[] trade;
    protected String v;
    protected HashSet<String> priceSet = new HashSet<>();
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
            v = order[2] + ",," + order[1] + "," + order[3] + "," + order[4] + ",," + 2;
            context.write(null, new Text(v));
        }

        // 处理LimitedOrder
        if (order[4].equals("2")) {
            v = order[2] + "," + order[0] + "," + order[1] + "," + order[3] + "," + order[4] + ",," + 2;
            context.write(null, new Text(v));
        }

        // 判断trade是否为空
        if (trade == null) {
            return;
        }

        // 处理Cancel
        if (trade[2].equals("4")) {
            v = trade[3] + ",," + trade[1] + "," +  order[3] + "," + order[4] + ",," + 1;
            context.write(null, new Text(v));
        }

        // 处理MarketOrder
        if (trade[2].equals("F")) {
            v = order[2] + ",," + order[1] + "," + order[3] + "," + order[4] + "," + priceSet.size() + "," + 2;
            context.write(null, new Text(v));
        }

    }
}
