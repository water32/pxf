package org.greenplum.pxf.api.serializer.converter;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class LocalDateTimeConverter implements ValueConverter<LocalDateTime, Long> {

    private static final LocalDateTime JavaEpoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);

    private static final LocalDateTime PostgresEpoch = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    private static final long DaysBetweenJavaAndPostgresEpochs = ChronoUnit.DAYS.between(JavaEpoch, PostgresEpoch);

    @Override
    public Long convert(final LocalDateTime dateTime) {

        if (dateTime == null) {
            throw new IllegalArgumentException("localDateTime");
        }
        // Extract the Time of the Day in Nanoseconds:
        long timeInNanoseconds = dateTime
                .toLocalTime()
                .toNanoOfDay();

        // Convert the Nanoseconds to Microseconds:
        long timeInMicroseconds = timeInNanoseconds / 1000;

        // Now Calculate the Postgres Timestamp:
        if (dateTime.isBefore(PostgresEpoch)) {
            long dateInMicroseconds = (dateTime.toLocalDate().toEpochDay() - DaysBetweenJavaAndPostgresEpochs) * 86400000000L;

            return dateInMicroseconds + timeInMicroseconds;
        } else {
            long dateInMicroseconds = (DaysBetweenJavaAndPostgresEpochs - dateTime.toLocalDate().toEpochDay()) * 86400000000L;

            return -(dateInMicroseconds - timeInMicroseconds);
        }
    }
}
