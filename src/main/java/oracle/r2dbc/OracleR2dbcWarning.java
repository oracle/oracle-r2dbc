package oracle.r2dbc;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;

import java.sql.SQLWarning;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.spi.Result.*;

/**
 * <p>
 * An exception that provides information on warnings raised by Oracle Database.
 * </p><p>
 * When a SQL command results in a warning, Oracle R2DBC generates
 * {@link Message} segments having an {@code OracleR2dbcWarning} as the
 * {@linkplain Message#exception() exception}. These segments may be consumed
 * with {@link Result#flatMap(Function)}:
 * </p><pre>{@code
 * result.flatMap(segment -> {
 *   if (OracleR2dbcWarning.isWarning(segment)) {
 *     logWarning(OracleR2dbcWarning.getWarning(segment));
 *     return emptyPublisher();
 *   }
 *   else {
 *     ... handle other segment types ...
 *   }
 * })
 * }</pre><p>
 * Alternatively, these segments may be ignored using
 * {@link Result#filter(Predicate)}:
 * </p> <pre>{@code
 * result.filter(segment -> !OracleR2dbcWarning.isWarning(segment))
 * }</pre>
 * If these segments are not flat-mapped or filtered from a {@code Result}, then
 * an {@code onError} signal with the {@code OracleR2dbcWarning} is emitted 
 * </p>
 */
public class OracleR2dbcWarning extends R2dbcException {

  /**
   * Constructs a new warning having a {@code SQLWarning} from JDBC as its
   * cause. The constructed warning has the same message, SQL state, and error
   * code as the given {@code SQLWarning}.
   * @param sql The SQL command that resulted in a warning. May be null.
   * @param sqlWarning The SQLWarning thrown by JDBC. Not null.
   */
  public OracleR2dbcWarning(String sql, SQLWarning sqlWarning) {
    super(sqlWarning.getMessage(), sqlWarning.getSQLState(),
      sqlWarning.getErrorCode(), sql, sqlWarning);
  }

  /**
   * Checks if a segment is a {@link Message} having an
   * {@code OracleR2dbcException} as an
   * {@linkplain Message#exception() exception}.
   * @param segment Segment to check. May be null, in which case {@code false}
   * is returned.
   * @return {@code true} if the segment is a {@code Message} with an
   * {@code OracleR2dbcException}, or {@code false} if not.
   */
  public static boolean isWarning(Segment segment) {
    return segment instanceof Message
      && ((Message)segment).exception() instanceof OracleR2dbcWarning;
  }

  /**
   * Returns the {@code OracleR2dbcWarning} of a {@link Message} segment. This
   * method should only be called if {@link #isWarning(Segment)} returned
   * {@code true} for the given segment.
   * @param segment A {@code Message} segment with an
   * {@code OracleR2dbcWarning}. Not null.
   * @return The {@code OracleR2dbcWarning} of the {@code Message} segment.
   * @throws IllegalArgumentException If the segment is not a {@code Message}
   * with an {@code OracleR2dbcWarning}.
   */
  public static OracleR2dbcWarning getWarning(Segment segment) {
    if (!isWarning(segment)) {
      throw new IllegalArgumentException(
        "Not a Message segment with an OracleR2dbcWarning: " + segment);
    }

    return (OracleR2dbcWarning)((Message)segment).exception();
  }

}
