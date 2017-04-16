package vlfsoft.common.nui.rxjdbc;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

import rx.Observable;
import rx.Subscriber;

final public class RxJdbcQuery {

    private RxJdbcQuery() {
    }

    public static <RowType> void onNext(Subscriber<? super RowType> aSubscriber, final ResultSet aResultSet, final ResultsetMapper<RowType> aResultsetMapper) {
        try {
            for (int i = 0; aResultSet.next(); i++) {
                aSubscriber.onNext(aResultsetMapper.map(aResultSet, i));
            }
        } catch (SQLException e) {
            aSubscriber.onError(e);
        }
        aSubscriber.onCompleted();
    }

    public static <RowType> Observable<RowType> observe(final ResultSet aResultSet, final ResultsetMapper<RowType> aResultsetMapper) {
        return Observable.unsafeCreate(subscriber -> {
            onNext(subscriber, aResultSet, aResultsetMapper);
        });
    }

    public static <RowType> Observable<RowType> observe(final Connection aConnection, final String aQuery, final ResultsetMapper<RowType> aResultsetMapper) {
        try (Statement statement = aConnection.createStatement(); ResultSet resultSet = statement.executeQuery(aQuery)) {
            return observe(resultSet, aResultsetMapper);
        } catch (SQLException e) {
            e.printStackTrace();
            return Observable.create(subscriber -> {
                subscriber.onError(e);
                subscriber.onCompleted();
            });
        }
    }

    public interface ResultsetMapper<RowType> {
        RowType map(ResultSet aResultSet, int aRow) throws SQLException;
    }

    public @interface Column {

        @Documented
        @Retention(RetentionPolicy.SOURCE)
        @Target({ElementType.METHOD})
        @interface Index {
            /**
             * @return value of Column index (1, ...) <a href="java.sql.ResultSet">ResultSet</a>.
             */
            int value();
        }

        @Documented
        @Retention(RetentionPolicy.SOURCE)
        @Target({ElementType.METHOD})
        @interface Label {
            /**
             * @return value of Column label in <a href="java.sql.ResultSet">ResultSet</a>.
             * If value == "", name of method without "get" is used. getName -> Name
             */
            String value() default "";
        }

    }

    // http://stackoverflow.com/questions/21162753/jdbc-resultset-i-need-a-getdatetime-but-there-is-only-getdate-and-gettimestamp
    public static Date getDateTime(final ResultSet aResultSet, int i) throws SQLException {
        Timestamp timestamp = aResultSet.getTimestamp(i);
        if (timestamp == null) return null;
        return new java.util.Date(timestamp.getTime());
    }

}