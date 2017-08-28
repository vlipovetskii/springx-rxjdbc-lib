package vlfsoft.common.spring.util;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import vlfsoft.common.nui.rxjdbc.RxJdbcQuery;
import vlfsoft.common.util.ExceptionUtils;

final public class SpringRxJdbcUtils {

    private SpringRxJdbcUtils() {
    }

    // TODO: Implement StreamResultSetExtractor.
    // Create SpringRxJdbcUtils in SpringRxJdbcLib and move there RX* specific methods and RxJava dependency.

    /**
     *
     * works with {@link ResultSet}
     * @param <RowType> -
     */
    public static class ObservableResultSetExtractor<RowType> implements ResultSetExtractor<Void> {

        final Subscriber<? super RowType> mSubscriber;
        final RxJdbcQuery.ResultsetMapper<RowType> mResultsetMapper;

        @Override
        public Void extractData(ResultSet aResultSet) throws SQLException, DataAccessException {
            RxJdbcQuery.onNext(mSubscriber, aResultSet, mResultsetMapper);
            return null;
        }

        public ObservableResultSetExtractor(final Subscriber<? super RowType> aSubscriber, final RxJdbcQuery.ResultsetMapper<RowType> aResultsetMapper) {
            mSubscriber = aSubscriber;
            mResultsetMapper = aResultsetMapper;
        }

    }

    /**
     * works with {@link SqlRowSet}
     * @param <T> -
     */
    public interface ResultsetMapper<T> {
        T map(SqlRowSet aResultSet, int aRow) throws InvalidResultSetAccessException;
    }

    /**
     * works with {@link SqlRowSet}
     * @param aResultSet -
     * @param aResultsetMapper -
     * @param <RowType> -
     * @return -
     */
    public static <RowType> Observable<RowType> observe(final SqlRowSet aResultSet, final ResultsetMapper<RowType> aResultsetMapper) {
        return Observable.unsafeCreate(subscriber -> {
            try {
                for (int i = 0; aResultSet.next(); i++) {
                    subscriber.onNext(aResultsetMapper.map(aResultSet, i));
                }
                // The main difference to the standard JDBC RowSet is that a SQLException is never thrown here.
                // This allows a SqlRowSet to be used without having to deal with checked exceptions.
                // A SqlRowSet will throw Spring's InvalidResultSetAccessException instead (when appropriate).
            } catch (InvalidResultSetAccessException e) {
                // http://blog.danlew.net/2015/12/08/error-handling-in-rxjava/
                // Checked Exceptions
                subscriber.onError(ExceptionUtils.newRuntimeException(e));
                subscriber.onCompleted();
                return;
            }
            subscriber.onCompleted();
        });
    }

}
