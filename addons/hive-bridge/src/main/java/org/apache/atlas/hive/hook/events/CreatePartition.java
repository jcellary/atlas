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

package org.apache.atlas.hive.hook.events;

import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.*;

public class CreatePartition extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreatePartition.class);

    public CreatePartition(AtlasHiveHookContext context) {
        super(context);
    }

    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        List<HookNotification> ret = null;
        AtlasEntitiesWithExtInfo entities = context.isMetastoreHook() ? getHiveMetastoreEntities() : getHiveEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getHiveMetastoreEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        ListenerEvent            event = context.getMetastoreEvent();
        HiveOperation            oper  = context.getHiveOperation();
        Partition partition;

        if (isCreatePartition(oper)) {
            partition = toPartition(((AddPartitionEvent) event).getPartitionIterator().next()); //FIXME: Add support for mutliple partitions
            processPartition(partition, ret);
        }

        addProcessedEntities(ret);

        return ret;
    }

    public AtlasEntitiesWithExtInfo getHiveEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        Partition                    partition = null;

        if (CollectionUtils.isNotEmpty(getOutputs())) {
            for (Entity entity : getOutputs()) {
                if (entity.getType() == Entity.Type.PARTITION) {
                    partition = entity.getPartition().getTPartition();

                    if (partition != null) {
                        partition = getHive()
                                .getPartition(getHive().getTable(partition.getTableName()), partition.getParameters(), false)
                                .getTPartition();
                    }
                }
            }
        }


        addProcessedEntities(ret);

        return ret;
    }

    private void processPartition(Partition partition, AtlasEntitiesWithExtInfo ret) throws Exception {
        LOG.debug(String.format("processPartition:Processing %s", partition.getValues()));
        if(partition != null){
            LOG.debug(String.format("processPartition: Processing %s", partition.getValues()));
            AtlasEntity partEntity = toPartitionEntity(partition, ret);
            if(partEntity != null) {
                AtlasEntity partDDLEntity = createHiveDDLEntity(partEntity);
                if (partDDLEntity != null) {
                    ret.addEntity(partEntity);
                }

            }
        }
    }


    private static boolean isCreatePartition(HiveOperation oper) {
        return (oper == ALTERTABLE_ADDPARTS);
    }


}
