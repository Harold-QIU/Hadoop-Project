package driver;

import mapper.OrderMapper;
import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import reducer.StockReducer;


import java.io.*;
import java.util.ArrayList;


public class StockAnalysis {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建配置对象和任务对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockAnalysis");
        job.setJarByClass(StockAnalysisLocal.class);

        // 设置第一个输入路径和对应的Map处理逻辑及输出类型: 目标为data/order目录下的所有文件
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderMapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型: 目标为data/trade目录下的所有文件
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TradeMapper.class);

        // 设置Reduce处理逻辑及输出类型
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // 提交任务并等待完成
        System.out.println("MapReduce Status:" + (job.waitForCompletion(true) ? 0 : 1));

        // 获取HDFS的文件系统对象
        FileSystem fs = FileSystem.get(conf);

        // 排序准备：读入文件
        Path pt = new Path(args[2], "part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        ArrayList<String> lines = new ArrayList<>();
        do {
            lines.add(br.readLine());
        } while (br.ready());

        // 重写排序规则：首先按照时间戳排序，然后按照订单号排序
        lines.sort((o1, o2) -> {
            String[] o1s = o1.split(",");
            String[] o2s = o2.split(",");
            if (!o1s[0].equals(o2s[0])) {
                return o1s[0].compareTo(o2s[0]);
            } else {
                return Integer.parseInt(o1s[5]) - Integer.parseInt(o2s[5]);
            }
        });

        // 加入表头
        String header = "TIMESTAMP,PRICE,SIZE,BUY_SELL_FLAG,ORDER_TYPE,ORDER_ID,MARKET_ORDER_TYPE,CANCEL_TYPE";
        lines.add(0, header);

        // 输出表主体到HDFS
        Path pt2 = new Path(args[2], "sorted.csv");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt2, true)));
        for (String line: lines) {
            bw.write(line);
            bw.newLine();
        }
        bw.flush();
    }

}
