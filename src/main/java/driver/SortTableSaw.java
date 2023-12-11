package driver;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

public class SortTableSaw {
    public static void main(String[] args) {
        ColumnType[] types = {ColumnType.STRING,
                ColumnType.STRING,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.STRING,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER};

        // Define column name
        String[] columnNames = {"TIMESTAMP", "PRICE", "SIZE", "BUY_SELL_FLAG", "ORDER_TYPE", "ORDER_ID", "MARKET_ORDER_TYPE", "CANCEL_TYPE"};

        CsvReadOptions.Builder builder = CsvReadOptions.builder("output/part-r-00000")
                .separator(',')
                .header(false)
                .columnTypes(types);

        CsvReadOptions options = builder.build();
        Table t1 = Table.read().usingOptions(options);

        // Rename the table column
        for (int i = 0; i < columnNames.length; i++) {
            t1.column(i).setName(columnNames[i]);
        }

        System.out.println(t1.structure());
        Table ascending = t1.sortAscendingOn("TIMESTAMP", "ORDER_ID");
        System.out.println(ascending.last(10));

        // output to csv
        ascending.write().csv("output/sorted.csv");

    }
}
