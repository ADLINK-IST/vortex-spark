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

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.omg.dds.demo.ShapeType;
import org.omg.dds.pub.DataWriter;
import org.omg.dds.topic.Topic;
import scala.Tuple2;
import vortex.demo.spark.VortexUtils;

import java.io.File;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static vortex.commons.util.VConfig.DefaultEntities.defaultPub;

public class VortexIShapesAnalytics {
    private static final int SHAPE_SIZE = 25;

    private static AtomicReference<DataWriter<ShapeType>> vortexWriter =
            new AtomicReference<>(null);

    public static void main(String[] args) {
        // URL of the Spark cluster
        final String sparkURL = "local[4]"; // use four threads on the local machine

        // The directory that will be used for check pointing
        final String defaultCheckpointDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "spark";
        final String checkPointDir =
                System.getProperty("vortex.spark.checkpointdir", defaultCheckpointDir);

        // Configure Spark and setup the Spark streaming context
        final SparkConf conf = new SparkConf().setAppName("VortexIShapesAnalytics").setMaster(sparkURL);
        final Duration batchDuration = new Duration(1000);
        final JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
        jssc.checkpoint(checkPointDir);


        final JavaReceiverInputDStream<ShapeType> stream =
                VortexUtils.createStream(jssc, "Circle",
                        IShapesEntityFactory.TYPE_NAME_TO_REGISTER, IShapesEntityFactory.SHAPETYPE_CLS, StorageLevel.MEMORY_ONLY());

//        final JavaDStream<ShapeType> filter = stream.filter(new Function<ShapeType, Boolean>() {
//            @Override
//            public Boolean call(ShapeType sample) throws Exception {
//                final int x = sample.x;
//                final int y = sample.y;
//                if (x == 0 || y == 0) {
//                    return Boolean.TRUE;
//                }
//                return Boolean.FALSE;
//            }
//        });

        JavaDStream<ShapeType> filter = stream.filter(circle -> circle.x == 0 || circle.y == 0);

//        final JavaPairDStream<String, Integer> tuples = filter.mapToPair(new PairFunction<ShapeType, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(ShapeType sample) throws Exception {
//                return new Tuple2<String, Integer>(sample.color, 1);
//            }
//        });

        JavaPairDStream<String, Integer> tuples = filter.mapToPair(circle -> new Tuple2<String, Integer>(circle.color, 1));

//        final JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer i1, Integer i2) throws Exception {
//                        return i1 + i2;
//                    }
//                },
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer i1, Integer i2) throws Exception {
//                        return (int) Math.round(i1 - (0.5) * i2);
//                    }
//                },
//                new Duration(60 * 1 * 1000),
//                new Duration(1 * 1000));

        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
                (i1, i2) -> i1 + 12, // aggregator
                (i1, i2) -> i1 - i2, // update for samples out of window
                new Duration(5 * 60 * 1000),
                new Duration(1 * 1000)
        );

//        final JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> in) throws Exception {
//                return in.swap();
//            }
//        });

        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(in -> in.swap());

//        final JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
//            @Override
//            public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
//                return in.sortByKey(false);
//            }
//        });

        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(in -> in.sortByKey());

//        sortedCounts.foreach(new Function<JavaPairRDD<Integer, String>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<Integer, String> rdd) throws Exception {
//                final DataWriter<ShapeType> dw = getWriter();
//                ShapeType sample = new ShapeType();
//                final StringBuilder sb = new StringBuilder("\nResults:\n");
//
//                for(Tuple2<Integer, String> next : rdd.take(10)) {
//                    String color = next._2;
//
//                    sample.color = color;
//                    sample.x = next._1 * 2;
//                    sample.y = colorToY(color);
//                    sample.shapesize = SHAPE_SIZE;
//                    dw.write(sample);
//                    sb.append(next.toString()).append("\n");
//                }
//
//                System.out.println(sb.toString());
//                return null;
//            }
//        });

        sortedCounts.foreach(rdd -> {
            rdd.take(10).forEach(next -> {
                String color = next._2;
                try {
                    getWriter().write(new ShapeType(color, next._1, colorToY(color), SHAPE_SIZE));
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            });
            return null;
        });

        jssc.start();
    }

    private static DataWriter<ShapeType> getWriter() {
        if (vortexWriter.get() == null) {
            final Topic<ShapeType> topic = IShapesEntityFactory.getTopic("Square");
            final DataWriter<ShapeType> writer = defaultPub().createDataWriter(topic);
            if (!vortexWriter.compareAndSet(null, writer)) {
                writer.close();
            }
        }

        return vortexWriter.get();
    }

    private static int colorToY(String color) {
        switch (color.toUpperCase()) {
            case "BLUE":
                return SHAPE_SIZE + 5;
            case "RED":
                return 2 * SHAPE_SIZE + 5;
            case "GREEN":
                return 3 * SHAPE_SIZE + 5;
            case "ORANGE":
                return 4 * SHAPE_SIZE + 5;
            case "YELLOW":
                return 5 * SHAPE_SIZE + 5;
            case "MAGENTA":
                return 6 * SHAPE_SIZE + 5;
            case "CYAN":
                return 7 * SHAPE_SIZE + 5;
            default:
                return 0;
        }
    }
}
