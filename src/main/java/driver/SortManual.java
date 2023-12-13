package driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SortManual {
    public static void main(String[] args) throws IOException {
        // 读入文件
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
