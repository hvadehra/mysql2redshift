package db;

/**
 * Created by hemanshu.v on 23/09/17.
 */
public class MysqlDao extends Dao {
    public MysqlDao(String url, int timeout) {
        super(url, "com.mysql.jdbc.Driver", timeout);
    }
}
