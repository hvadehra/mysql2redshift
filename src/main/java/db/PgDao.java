package db;

/**
 * Created by hemanshu.v on 23/09/17.
 */
public class PgDao extends Dao {
    public PgDao(String url, int timeout) {
        super(url, "org.postgresql.Driver", timeout);
    }
}
