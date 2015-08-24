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
import org.apache.spark.streaming.receiver.Receiver;
import org.omg.dds.core.event.DataAvailableEvent;
import org.omg.dds.core.policy.Durability;
import org.omg.dds.core.policy.DurabilityService;
import org.omg.dds.core.policy.History;
import org.omg.dds.core.status.Status;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.DataReaderQos;
import org.omg.dds.sub.Sample;
import org.omg.dds.sub.SampleState;
import org.omg.dds.topic.Topic;
import org.omg.dds.topic.TopicQos;
import org.omg.dds.type.TypeSupport;
import vortex.commons.util.BaseDataReaderListener;
import vortex.commons.util.VConfig;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static vortex.commons.util.VConfig.DefaultEntities.*;

public class VortexReceiver<TYPE> extends Receiver<TYPE> {
    public static final int DS_HISTORY = 100;
    public static final int DS_MAX_SAMPLES = 8192;
    private static final int DS_MAX_INSTANCES = 4196;
    private static final int DS_MAX_SAMPLES_X_INSTANCE = 8192;
    private static final int DS_CLEAN_UP_DELAY = 3600;

    private final String topicName;
    private final String topicRegType;
    private final Class<TYPE> topicType;
    private DataReader<TYPE> dr;

    public VortexReceiver(StorageLevel storageLevel, String topic, String topicRegType, Class<TYPE> topicType) {
        super(storageLevel);
        this.topicName = topic;
        this.topicRegType = topicRegType;
        this.topicType = topicType;
    }

    @Override
    public void onStart() {
        final TypeSupport<TYPE> typeSupport = TypeSupport.newTypeSupport(topicType, topicRegType, VConfig.ENV);

        final Durability durability = defaultPolicyFactory().Durability().withPersistent();
        final DurabilityService durabilityService = defaultPolicyFactory().DurabilityService()
                .withHistoryDepth(DS_HISTORY)
                .withServiceCleanupDelay(DS_CLEAN_UP_DELAY, TimeUnit.SECONDS)
                .withHistoryKind(History.Kind.KEEP_LAST)
                .withMaxInstances(DS_MAX_INSTANCES)
                .withMaxSamples(DS_MAX_SAMPLES)
                .withMaxSamplesPerInstance(DS_MAX_SAMPLES_X_INSTANCE);
        final TopicQos topicQos = defaultDomainParticipant().getDefaultTopicQos()
                .withPolicy(durability)
                .withPolicy(durabilityService);
        final History history = defaultPolicyFactory().History().withKeepLast(100 * 2);
        final DataReaderQos drQos = defaultSub().getDefaultDataReaderQos().withPolicy(history);


        final Topic<TYPE> topic =
                defaultDomainParticipant().createTopic(topicName, typeSupport, topicQos, null, (Collection<Class<? extends Status>>) null);
        dr = defaultSub().createDataReader(topic, drQos);

        dr.setListener(new BaseDataReaderListener<TYPE>() {
            @Override
            public void onDataAvailable(DataAvailableEvent<TYPE> dataAvailableEvent) {
                final DataReader.Selector<TYPE> selector = dr.select();
                selector.dataState(selector.getDataState().with(SampleState.NOT_READ));
                final Sample.Iterator<TYPE> samples = dr.take(selector);
                try {
                    while (samples.hasNext()) {
                        final Sample<TYPE> sample = samples.next();
                        TYPE data = sample.getData();
                        if (data != null) {
                            VortexReceiver.this.store(data);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void onStop() {

        if (dr != null) {
            dr.setListener(null);
            dr.close();
            dr = null;
        }
    }

}
