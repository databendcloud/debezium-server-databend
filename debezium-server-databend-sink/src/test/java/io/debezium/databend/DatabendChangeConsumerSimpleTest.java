/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend;

import io.debezium.databend.testresources.BaseDbTest;
import io.debezium.databend.testresources.TargetDatabendDB;
import io.debezium.databend.testresources.TestChangeEvent;
import io.debezium.databend.testresources.TestUtil;
import io.debezium.server.databend.DatabendChangeConsumer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hantmac
 */
@QuarkusTest
@QuarkusTestResource(TargetDatabendDB.class)
public class DatabendChangeConsumerSimpleTest extends BaseDbTest {
    @Inject
    DatabendChangeConsumer consumer;

    @Test
    public void testSimpleUpload() throws Exception {
        consumer.connection = new TargetDatabendDB().createConnection();

        String dest = "customers_append";
        List<io.debezium.engine.ChangeEvent<Object, Object>> records = new ArrayList<>();
        records.add(TestChangeEvent.of(dest, 1, "c"));
        records.add(TestChangeEvent.of(dest, 2, "c"));
        records.add(TestChangeEvent.of(dest, 3, "c"));
        consumer.handleBatch(records, TestUtil.getCommitter());

        // check that its consumed!
        // 3 records should be updated 4th one should be inserted
        records.clear();
        records.add(TestChangeEvent.of(dest, 1, "r"));
        records.add(TestChangeEvent.of(dest, 2, "d"));
        records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV1"));
        records.add(TestChangeEvent.of(dest, 4, "c"));
        consumer.handleBatch(records, TestUtil.getCommitter());
    }

    @AfterEach
    public void clearData() throws SQLException, ClassNotFoundException {
        ResultSet rs = select("delete from public.debeziumcdc_customers_append");
    }
}

