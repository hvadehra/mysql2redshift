import db.Dao;
import db.MysqlDao;
import db.PgDao;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by hemanshu.v on 23/09/17.
 */
public class Main {
    private final static String targetDir = "/tmp";
    private final static int timeout = 3000;

    public static void main(String[] args) {
        String url = getOpt(args, 0).orElseThrow(Main::fail);
        String type = getOpt(args, 1).orElseThrow(Main::fail);
        String dbName = getOpt(args, 2).orElseThrow(Main::fail);
        String table = getOpt(args, 3).orElseThrow(Main::fail);
        int limit = Integer.valueOf(getOpt(args, 4).orElse("10000"));
        int offset = Integer.valueOf(getOpt(args, 5).orElse("0"));

        Dao dao = getDao(url, type, timeout);
        try {
            new App(dao, dbName, targetDir).start(table, limit, offset);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static RuntimeException fail() {
        return new RuntimeException("java Main <url> <mysql|pg> <db> <table> [limit offset]");
    }

    private static Optional<String> getOpt(String[] args, int index) {
        if (index < args.length){
            return Optional.ofNullable(args[index]);
        }
        return Optional.empty();
    }

    private static Dao getDao(String url, String type, int timeout) {
        if ("mysql".equals(type)){
            return new MysqlDao(url, timeout);
        }
        if ("pg".equals(type)){
            return new PgDao(url, timeout);
        }
        throw new RuntimeException("Invalid db type");
    }
}
