/**
 * PrismTech licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License and with the PrismTech Vortex product. You may obtain a copy of the
 * License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License and README for the specific language governing permissions and
 * limitations under the License.
 */
package vortex.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.omg.dds.pub.DataWriter;
import org.omg.dds.topic.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import twitter4j.Status;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static vortex.commons.util.VConfig.DefaultEntities.defaultDomainParticipant;
import static vortex.commons.util.VConfig.DefaultEntities.defaultPub;

public class VortexSparkTwitterDemo {
    private static Logger LOG = LoggerFactory.getLogger("vortex.demo");

    private static final CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();
    private static AtomicReference<DataWriter<TopTenHashtagsType>> vortexWriter =
            new AtomicReference<>(null);

    private static final String[] FILTER = {"iot"};
    private static final String KEY = StringUtils.join(FILTER, ";");

    public static void main(String[] args) {

        // URL of the Spark cluster
        final String sparkURL = "local[4]"; // use four threads on the local machine

        // The directory that will be used for check pointing
        final String defaultCheckpointDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "spark";
        final String checkPointDir =
                System.getProperty("vortex.spark.checkpointdir", defaultCheckpointDir);

        if (!configureTwitterCredentials()) {
            LOG.info("Unable to configure Twitter credentials exiting.");
            System.exit(1);
        }

        // Configure Spark and setup the Spark streaming context
        final SparkConf conf = new SparkConf().setAppName("VortexSparkTwitterDemo").setMaster(sparkURL);
        final Duration batchDuration = new Duration(1000);
        final JavaStreamingContext streamingContext = new JavaStreamingContext(conf, batchDuration);
        streamingContext.checkpoint(checkPointDir);

        // Create a new stream of
        // "iot", "Internet of Things", "M2M", "mqtt"
        final JavaDStream<Status> tweets
                = TwitterUtils.createStream(
                streamingContext, FILTER, StorageLevel.MEMORY_ONLY());

        final JavaDStream<String> statuses = tweets.map(new Function<Status, String>() {
            @Override
            public String call(Status status) throws Exception {
                return status.getText();
            }
        });

        final JavaDStream<String> words = statuses.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        final JavaDStream<String> hashtags = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) throws Exception {
                return word.startsWith("#");
            }
        });

        final JavaPairDStream<String, Integer> tuples = hashtags.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        final JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 - i2;
                    }
                },
                new Duration(60 * 5 * 1000),
                new Duration(1 * 1000)
        );

        final JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> in) throws Exception {
                return in.swap();
            }
        });

        final JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
            @Override
            public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
                return in.sortByKey(false);
            }
        });

        sortedCounts.foreach(new Function<JavaPairRDD<Integer, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, String> rdd) throws Exception {
                int idx = 0;
                int[] count = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                String[] hashtag = {"", "", "", "", "", "", "", "", "", ""};
                StringBuilder sb = new StringBuilder("\nTop 10 hashtags:\n");

                for (Tuple2<Integer, String> next : rdd.take(10)) {
                    count[idx] = next._1();
                    try {
                        final ByteBuffer encoded = encoder.encode(CharBuffer.wrap(next._2()));
                        hashtag[idx] = new String(encoded.array(), "US-ASCII");
                    } catch (Exception ex) {
                        hashtag[idx] = "Encoding error";
                    }
                    idx++;
                    sb.append(next.toString()).append("\n");
                }

                final TopTenHashtagsType stats = new TopTenHashtagsType(KEY, count, hashtag);
                System.out.println(sb.toString());
                getWriter().write(stats);

                return null;
            }
        });

        streamingContext.start();
    }

    private static boolean configureTwitterCredentials() {
        final String[] configKeys = {"consumerKey", "consumerSecret", "accessToken", "accessTokenSecret"};
        for (String key : configKeys) {
            final String value = System.getProperty(key);

            if (StringUtils.isEmpty(value)) {
                LOG.error("Error setting OAuth authentication - value for " + key + " not found.");
                return false;
            } else {
                final String oauthKey = "twitter4j.oauth." + key;
                System.setProperty(oauthKey, value);
                LOG.info("\t{} set as {}", oauthKey, value);
            }
        }

        return true;
    }

    private static DataWriter<TopTenHashtagsType> getWriter() {
        if (vortexWriter.get() == null) {
            final Topic<TopTenHashtagsType> topic = defaultDomainParticipant().createTopic("TopTenHashtags", TopTenHashtagsType.class);
            final DataWriter<TopTenHashtagsType> writer = defaultPub().createDataWriter(topic);
            if (!vortexWriter.compareAndSet(null, writer)) {
                writer.close();
            }
        }

        return vortexWriter.get();
    }
}
