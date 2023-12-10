package driver;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvWriteOptions;
import tech.tablesaw.io.csv.CsvWriter;

public class SortTableSaw {
    public static void main(String[] args) {
        ColumnType[] types = {ColumnType.STRING,
                ColumnType.STRING,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.STRING,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,};

        CsvReadOptions.Builder builder = CsvReadOptions.builder("output/part-r-00000")
                .separator(',')
                .header(false)
                .columnTypes(types);

        CsvReadOptions options = builder.build();
        Table t1 = Table.read().usingOptions(options);
        System.out.println(t1.structure());
        Table ascending = t1.sortAscendingOn("C0", "C5");
        System.out.println(ascending.last(10));
        // output to csv
        CsvWriteOptions writeOptions = CsvWriteOptions.builder("output/sorted.csv")
                .header(false)
                .build();
        CsvWriter writer = new CsvWriter();
        writer.write(ascending, writeOptions);

    }
}
