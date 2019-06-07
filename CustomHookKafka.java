/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.notification.hook.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * AtlasHook sends lineage information to the AtlasSever.
 */
public class CustomHookKafka extends AtlasHook {

    public static final String CONF_PREFIX          = "atlas.hook.custom.";
    public static final String HOOK_NUM_RETRIES     = CONF_PREFIX + "numRetries";
    public static final String ATLAS_CLUSTER_NAME   = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";

        @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }


    public void publish(String owner) throws AtlasHookException {
        try {
            Configuration atlasProperties = ApplicationProperties.get();
            String        clusterName     = atlasProperties.getString(ATLAS_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
            AtlasEntity   entresearchPaperAccessDataset  = createResearchPaperAccessDataset(clusterName, owner);
            AtlasEntity   entresearchPaperRecommendationResults = createResearchPaperRecommendationResults(clusterName, owner);
            AtlasEntity   entProcess      = createMLProcessInstance(entresearchPaperAccessDataset, entresearchPaperRecommendationResults, clusterName, owner);

            AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo(entProcess);

            entities.addReferredEntity(entresearchPaperAccessDataset);
            entities.addReferredEntity(entresearchPaperRecommendationResults);
            entities.addReferredEntity(entProcess);

            HookNotificationMessage message  = new EntityUpdateRequestV2(AtlasHook.getUser(), entities);

            AtlasHook.notifyEntities(Arrays.asList(message), atlasProperties.getInt(HOOK_NUM_RETRIES, 3));
        } catch(Exception e) {
            throw new AtlasHookException("CustomHookKafka.publish() failed.", e);
        }
    }

    private AtlasEntity createResearchPaperAccessDataset(String clusterName, String owner) {
        AtlasEntity entresearchPaperAccessDataset  = new AtlasEntity("ResearchPaperAccessDataset");

        entresearchPaperAccessDataset.setAttribute("resourceSetID", "1224");
        entresearchPaperAccessDataset.setAttribute("researchPaperGroupName", "WV-SP-INT-HWX");
        entresearchPaperAccessDataset.setAttribute("createTime", "2017-03-25T20:07:12.000Z");
        entresearchPaperAccessDataset.setAttribute(AtlasClient.NAME, "GeoThermal-1224");
        entresearchPaperAccessDataset.setAttribute(AtlasClient.DESCRIPTION, "GeoThermal Research Input Dataset 1224");
        entresearchPaperAccessDataset.setAttribute(AtlasClient.QUALIFIED_NAME, "ResearchPaperAccessDataset.1224-WV-SP-INT-HWX");
        entresearchPaperAccessDataset.setAttribute(AtlasClient.OWNER, owner);
        entresearchPaperAccessDataset.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, "c370");

        return entresearchPaperAccessDataset;
    }

    private AtlasEntity createResearchPaperRecommendationResults(String clusterName, String owner) {
        AtlasEntity entresearchPaperRecommendationResults  = new AtlasEntity("ResearchPaperRecommendationResults");

        entresearchPaperRecommendationResults.setAttribute("recommendationsResultsetID", "4995149");
        entresearchPaperRecommendationResults.setAttribute("researchArea", "GeoThermal");
        entresearchPaperRecommendationResults.setAttribute("createTime", "2017-03-25T21:00:12.000Z");
        entresearchPaperRecommendationResults.setAttribute("hdfsDestination", "hdfs://hdpcluster:8020/edm/data/prod/recommendations");
        entresearchPaperRecommendationResults.setAttribute(AtlasClient.NAME, "RecommendationsGeoThermal-4995149");
        entresearchPaperRecommendationResults.setAttribute(AtlasClient.DESCRIPTION, "GeoThermal Research Input Dataset 1224");
        entresearchPaperRecommendationResults.setAttribute(AtlasClient.QUALIFIED_NAME, "ResearchPaperRecommendationResults.GeoThermal-4995149");
        entresearchPaperRecommendationResults.setAttribute(AtlasClient.OWNER, owner);
        entresearchPaperRecommendationResults.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, "c370");

        return entresearchPaperRecommendationResults;


    }

    private AtlasEntity createMLProcessInstance(AtlasEntity entresearchPaperAccessDataset, AtlasEntity entresearchPaperRecommendationResults, String clusterName, String owner) {
        AtlasEntity         entProcess       = new AtlasEntity("ResearchPaperMachineLearning");

        entProcess.setAttribute(AtlasClient.NAME, "ML_Iteration567019");
        entProcess.setAttribute(AtlasClient.QUALIFIED_NAME, "ResearchPaperMachineLearning.ML_Iteration567019");
        entProcess.setAttribute(AtlasClient.DESCRIPTION, "GeoThermal Research Input Dataset 1224");
        entProcess.setAttribute(AtlasClient.OWNER, owner);
        entProcess.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, "c370");

        entProcess.setAttribute("inputs", Arrays.asList(AtlasTypeUtil.getAtlasObjectId(entresearchPaperAccessDataset)));
        entProcess.setAttribute("outputs", Arrays.asList(AtlasTypeUtil.getAtlasObjectId(entresearchPaperRecommendationResults)));

        entProcess.setAttribute("startTime","2017-03-26T20:20:13.675Z");
        entProcess.setAttribute("endTime", "2017-03-26T20:27:23.675Z");
        entProcess.setAttribute("userName", "hdpdev-edm-appuser-recom");
        entProcess.setAttribute("operationType", "DecisionTreeAndRegression");
        entProcess.setAttribute("clusterName", "c370");
        entProcess.setAttribute("queryGraph", "");

        return entProcess;
    }

    public static void main(String args[]) {
         CustomHookKafka obj1 = new CustomHookKafka();
         try {
                 obj1.publish(args[0]);
            }  catch(Exception e) {
            System.out.println("CustomHookKafka.publish() failed. "+ e);
        }
    }

}

