package driver;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.sql.SQLOutput;

public class SortTest {
    public static void main(String[] args) {
        ColumnType[] types = {ColumnType.LOCAL_DATE_TIME,
                ColumnType.DOUBLE,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.STRING,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,};

        CsvReadOptions.Builder builder = CsvReadOptions.builder("output/part-r-00000")
                .separator(',')
                .header(false)
                .dateTimeFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                .columnTypes(types);

        CsvReadOptions options = builder.build();
        Table t1 = Table.read().usingOptions(options);
        System.out.println(t1.structure());
        Table ascending = t1.sortAscendingOn("C0", "C5");
        System.out.println(ascending.first(10));
    }
}
