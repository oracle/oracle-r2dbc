package oracle.r2dbc.samples;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.reactivestreams.Publisher;

import java.time.LocalDate;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class PipelineDemo {

  private static final String TABLE_NAME = "oracle_r2dbc_pipeline_demo";

  static ConnectionFactory getConnectionFactory() {
    return ConnectionFactories.get(ConnectionFactoryOptions.builder()
      .option(DRIVER, "oracle")
      .option(HOST, DatabaseConfig.HOST)
      .option(PORT, DatabaseConfig.PORT)
      .option(DATABASE, DatabaseConfig.SERVICE_NAME)
      .option(USER, DatabaseConfig.USER)
      .option(PASSWORD, DatabaseConfig.PASSWORD)
      .build());
  }

  static Publisher<Void> executeTransaction(long userId) {}

  private static final class City {

    /* The name of this city, such as "San Francisco" */
    private final String name;

    /**
     * The longitude of this city, as a degree, such as -122. Positive numbers
     * are east of the Prime Meridian, and negative numbers are west of it.
     */
    private final int longitude;

    /**
     * The longitude of this city, as a degree, such as 37. Positive numbers
     * are north of the equator, and negative numbers are south of it.
     */
    private final int latitude;

    /**
     * The date when this city was found.
     */
    private final LocalDate dateFounded;

    private City(
      String name, int longitude, int latitude, LocalDate dateFounded) {
      this.name = name;
      this.longitude = longitude;
      this.latitude = latitude;
      this.dateFounded = dateFounded;
    }

  }

  private static final class Country {

    /** The name of this country, such as "United States of America" */
    private final String name;

    /** The capital city of this country */
    private final City capital;

    private Country(String name, City capital) {
      this.name = name;
      this.capital = capital;
    }
  }


}
