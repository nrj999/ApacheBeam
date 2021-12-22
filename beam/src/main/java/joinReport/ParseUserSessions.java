package joinReport;



import joinReport.UserSession;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class ParseUserSessions extends DoFn<String,UserSession> {

    private static final String[] FILE_HEADER_MAPPING = {
    		"UserId","ItemID"
    };

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        final CSVParser parser = new CSVParser(new StringReader(c.element()), CSVFormat.DEFAULT
                .withDelimiter(',')
                .withHeader(FILE_HEADER_MAPPING));
        CSVRecord record = parser.getRecords().get(0);



        UserSession us = new UserSession();
        us.setUserId(record.get("UserId"));
        us.setItemID(record.get("ItemID"));
     
        c.output(us);
    }
}