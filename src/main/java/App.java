import db.Dao;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by hemanshu.v on 23/09/17.
 */
class App {

    private final static String fileNamePattern = "$TABLE.$ROW_NUM.json";
    private static final int BUFFER_SIZE = 102400;

    private final Dao dao;
    private final String dbName;
    private final String targetDir;

    App(Dao dao, String dbName, String targetDir) {
        this.dao = dao;
        this.dbName = dbName;
        this.targetDir = targetDir;
    }

    void start(String table, int limit, int offset) throws IOException {
        List<String> columns = dao.getColumns(dbName, table);
        List<String> primaryKeys = dao.getPrimaryKeys(dbName, table);
        if (!primaryKeys.isEmpty()) {
            fetchInChunks(table, columns, primaryKeys, limit, offset);
        } else {
            dao.readAllRows(table, new MyRowCallBack(table, columns, limit));
        }
    }

    private void fetchInChunks(String table, List<String> columns, List<String> primaryKeys, int limit, int offset) throws IOException {
        do {
            String fileName = prepareFileName(table, offset);
            List<List<Object>> rows = dao.readChunk(table, columns, primaryKeys, limit, offset);
            System.out.println("Read " + rows.size() + " rows");
            if (rows.isEmpty()) break;
            FileWriter fileWriter = new FileWriter(fileName);
            BufferedWriter writer = new BufferedWriter(fileWriter);
            rows.stream()
                    .map(row -> rowAsJsonLine(row, columns))
                    .forEach(rowStr -> {
                        try {
//                            System.out.println(rowStr);
                            writer.write(rowStr);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                    });
            offset = offset + limit;
            writer.close();
            fileWriter.close();
            if (rows.size() < limit){
                break;
            }
        } while (true);
    }

    private class MyRowCallBack implements RowCallbackHandler {
        private final String table;
        private final List<String> columns;
        private final int limit;
        private int rowNum = 0;
        private BufferedWriter writer = null;
        private FileWriter fileWriter = null;

        private MyRowCallBack(String table, List<String> columns, int limit) {
            this.table = table;
            this.columns = columns;
            this.limit = limit;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            try {
                if (rowNum % limit == 0) {
                    switchToNewFile();
                }
                List<Object> row = Dao.getListRowMapper(columns).mapRow(rs, rowNum);
                writer.write(rowAsJsonLine(row, columns));
                rowNum++;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        private void switchToNewFile() throws IOException {
            if (null != writer) {
                writer.close();
            }
            if (null != fileWriter) {
                fileWriter.close();
            }
            String fileName = prepareFileName(table, rowNum);
            fileWriter = new FileWriter(fileName);
            writer = new BufferedWriter(fileWriter, BUFFER_SIZE);
        }
    }

    private String prepareFileName(String table, int rowNum) {
        return targetDir + "/" + fileNamePattern
                .replace("$TABLE", table)
                .replace("$ROW_NUM", String.valueOf(rowNum));
    }

    private String rowAsJsonLine(List<Object> row, List<String> columns) {
//        System.out.println("Col count in row: " + row.size() + "," + "actual cols: " + columns);
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("{");
        for (int i = 0; i < row.size(); i++) {
            String colName = columns.get(i);
            Optional<String> colValueOpt = Optional.ofNullable(row.get(i)).map(Object::toString);
//            System.out.println("(c,v) : (" + colName + "," + colValueOpt + ")");
            int finalI = i;
            colValueOpt.ifPresent(colValue -> {
                if (finalI > 0) {
                    sbuf.append(",");
                }
                sbuf
                        .append("\"")
                        .append(colName)
                        .append("\" : \"")
                        .append(colValue)
                        .append("\"");
            });
        }
        sbuf.append("}\n");
        return sbuf.toString();
    }
}
