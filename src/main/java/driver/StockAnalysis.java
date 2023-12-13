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
import reducer.StockReducer;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class StockAnalysis {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建配置对象和任务对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockAnalysis");
        job.setJarByClass(StockAnalysis.class);

        // 设置第一个输入路径和对应的Map处理逻辑及输出类型: 目标为data/order目录下的所有文件
        MultipleInputs.addInputPath(job, new Path("data/order"), TextInputFormat.class, OrderMapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型: 目标为data/trade目录下的所有文件
        MultipleInputs.addInputPath(job, new Path("data/trade"), TextInputFormat.class, TradeMapper.class);

        // 设置Reduce处理逻辑及输出类型
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        // 提交任务并等待完成
        System.out.println("MapReduce Status:" + (job.waitForCompletion(true) ? 0 : 1));

        // 排序准备：读入文件
        List<String> lines = Files.readAllLines(Paths.get("output/part-r-00000"));

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

        // 输出到文件
        Files.write(Paths.get("output/sorted.csv"), lines);
    }

}
