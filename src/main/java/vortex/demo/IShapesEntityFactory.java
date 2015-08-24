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

import org.omg.dds.core.policy.Durability;
import org.omg.dds.core.policy.DurabilityService;
import org.omg.dds.core.policy.History;
import org.omg.dds.core.status.Status;
import org.omg.dds.demo.ShapeType;
import org.omg.dds.topic.Topic;
import org.omg.dds.topic.TopicQos;
import org.omg.dds.type.TypeSupport;
import vortex.commons.util.VConfig;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static vortex.commons.util.VConfig.DefaultEntities.defaultDomainParticipant;
import static vortex.commons.util.VConfig.DefaultEntities.defaultPolicyFactory;

/**
 * Created by tmcclean on 15-04-02.
 */
public final class IShapesEntityFactory {
    static final String TYPE_NAME_TO_REGISTER =
            System.getProperty("dds.registerType", "ShapeType");

    public static final Class<ShapeType> SHAPETYPE_CLS = ShapeType.class;

    public static final int DS_HISTORY = 100;
    public static final int DS_MAX_SAMPLES = 8192;
    private static final int DS_MAX_INSTANCES = 4196;
    private static final int DS_MAX_SAMPLES_X_INSTANCE = 8192;
    private static final int DS_CLEAN_UP_DELAY = 3600;

    private IShapesEntityFactory() {
    }

    public static TopicQos getTopicQos() {
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

        return topicQos;
    }

    public static Topic<ShapeType> getTopic(String topicName) {
        final TypeSupport<ShapeType> typeSupport = TypeSupport.newTypeSupport(SHAPETYPE_CLS, TYPE_NAME_TO_REGISTER, VConfig.ENV);
        return defaultDomainParticipant().createTopic(topicName, typeSupport, getTopicQos(), null, (Collection<Class<? extends Status>>) null);
    }

}
