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
package vortex.demo.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class VortexUtils {
    public static <TYPE> JavaReceiverInputDStream<TYPE> createStream(JavaStreamingContext jssc,
                                                                     String topic, String topicRegType, Class<TYPE> topicType,
                                                                     StorageLevel storageLevel) {
        return jssc.receiverStream(new VortexReceiver<>(storageLevel, topic, topicRegType, topicType));
    }
}
