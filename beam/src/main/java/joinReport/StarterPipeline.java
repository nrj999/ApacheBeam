package joinReport;


import joinReport.ParseUserSessions;
import joinReport.ParseProduct;
import joinReport.UserSession;


import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;


import javax.annotation.Nullable;



public class StarterPipeline {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<UserSession> userSessions = p
                .apply("ReadUserSessions",
                        TextIO.read().from("gs://bucket1test_nj/INPUT/user-purchases.txt"))
                .apply("ParseUserSessions",
                        ParDo.of(new ParseUserSessions()));
              

        PCollection<KV<String,String>> products = p
                .apply("ReadVideoTitles",
                        TextIO.read().from("gs://bucket1test_nj/INPUT/products.txt"))
                .apply("ParseVideoTitles",
                        ParDo.of(new ParseProduct()))
          ;

       
     
       


        
        
        PCollection<KV<String, UserSession>> keyedUserSessions = userSessions
                .apply("MapByUserId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(UserSession.class)))
                        .via(us -> KV.of(us.getUserId(), us)));
        
        

        TupleTag<String> titleTag = new TupleTag<>();
        TupleTag<UserSession> sessionTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> sessionAndTitle = KeyedPCollectionTuple
                .of(titleTag, products)
                .and(sessionTag, keyedUserSessions)
                .apply(CoGroupByKey.create());

        PCollection<UserSession> userSessionsWithTitle = sessionAndTitle
                .apply("AddTitleToUserSession",
                        ParDo.of(new DoFn<KV<String,CoGbkResult>, UserSession>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String cost = "";
                                for(String t: c.element().getValue().getAll(titleTag)) {
                                    cost = t;
                                }
                                for(UserSession us : c.element().getValue().getAll(sessionTag)){
                                    UserSession out = SerializationUtils.clone(us);
                                    out.setCost(cost);
                                    c.output(out);
                                }
                            }
                        }));

        userSessionsWithTitle
                .apply("ToStrings", MapElements
                        .into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRowWithTitle(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("outputWithTitles")

                        .withNumShards(1)
                        .withWindowedWrites());

        p.run();

    }
}