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

import org.omg.dds.core.event.*;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.DataReaderListener;
import org.omg.dds.sub.Sample;
import org.omg.dds.topic.Topic;
import vortex.commons.util.VConfig;

public class VortexHashtagReporter {

    public static void main(String[] args) {
        final Topic<TopTenHashtagsType> topic = VConfig.DefaultEntities.defaultDomainParticipant().createTopic("TopTenHashtags", TopTenHashtagsType.class);

        final DataReader<TopTenHashtagsType> reader = VConfig.DefaultEntities.defaultSub().createDataReader(topic);

        reader.setListener(new DataReaderListener<TopTenHashtagsType>() {
            @Override
            public void onRequestedDeadlineMissed(RequestedDeadlineMissedEvent<TopTenHashtagsType> requestedDeadlineMissedEvent) {

            }

            @Override
            public void onRequestedIncompatibleQos(RequestedIncompatibleQosEvent<TopTenHashtagsType> requestedIncompatibleQosEvent) {

            }

            @Override
            public void onSampleRejected(SampleRejectedEvent<TopTenHashtagsType> sampleRejectedEvent) {

            }

            @Override
            public void onLivelinessChanged(LivelinessChangedEvent<TopTenHashtagsType> livelinessChangedEvent) {

            }

            @Override
            public void onDataAvailable(DataAvailableEvent<TopTenHashtagsType> data) {
                final Sample.Iterator<TopTenHashtagsType> samples = reader.take();
                if (samples != null) {

                    while (samples.hasNext()) {

                        final Sample<TopTenHashtagsType> next = samples.next();
                        final TopTenHashtagsType hashtags = next.getData();
                        if (hashtags != null) {
                            StringBuilder sb = new StringBuilder("\nTop 10 hashtags: ");
                            sb.append(hashtags.filter).append("\n");
                            for (int i = 0; i < hashtags.hashtag.length; i++) {
                                sb.append(i).append(". (");
                                sb.append(hashtags.count[i]).append(", ").append(hashtags.hashtag[i]);
                                sb.append(")\n");
                            }
                            System.out.println(sb.toString());
                        }
                    }
                }
            }

            @Override
            public void onSubscriptionMatched(SubscriptionMatchedEvent<TopTenHashtagsType> subscriptionMatchedEvent) {

            }

            @Override
            public void onSampleLost(SampleLostEvent<TopTenHashtagsType> sampleLostEvent) {

            }
        });
    }
}
