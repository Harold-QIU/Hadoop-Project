package driver;

import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

public class SortTest {
    public static void main(String[] args) {
        CsvReadOptions.Builder builder =
                CsvReadOptions.builder("output/part-r-00000")
                        .separator(',')										// table is tab-delimited
                        .header(false)											// no header
                        .dateFormat("M/d/yyyy h:mm:ss a");
        CsvReadOptions options = builder.build();
        Table t1 = Table.read().usingOptions(options);
        System.out.println(t1.structure());
    }
}
