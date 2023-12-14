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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.jooq.meta.derby.sys.Sys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hantmac
 */
@QuarkusTest
@QuarkusTestResource(TargetDatabendDB.class)
@TestProfile(DatabendChangeConsumerUpsertTest.DatabendChangeConsumerUpsertTestProfile.class)
public class DatabendChangeConsumerUpsertTest extends BaseDbTest {
    @Inject
    DatabendChangeConsumer consumer;

    @Test
    public void testSimpleUpload() throws Exception {
        consumer.connection = new TargetDatabendDB().createConnection();

        String dest = "customers_upsert";
        List<io.debezium.engine.ChangeEvent<Object, Object>> records = new ArrayList<>();
        records.add(TestChangeEvent.of(dest, 1, "c"));
        records.add(TestChangeEvent.of(dest, 2, "c"));
        records.add(TestChangeEvent.of(dest, 3, "c"));
        consumer.handleBatch(records, TestUtil.getCommitter());
        // check that its consumed!
        ResultSet rs = getDatabendTableData("select * from public.debeziumcdc_customers_upsert");
        Assertions.assertEquals(getResultSetRowCount(rs), 3);
        ResultSet rs1 = getDatabendTableData("select * from public.debeziumcdc_customers_upsert where id =3");
        if (rs1.next()) {
            int id = rs1.getInt("id");
            Assertions.assertEquals(3, id);
        } else {
            throw new Exception("failed to get correct data");
        }

        // 3 records should be updated 4th one should be inserted
        records.clear();
        records.add(TestChangeEvent.of(dest, 1, "r"));
        records.add(TestChangeEvent.of(dest, 2, "d"));
        records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV1"));
        records.add(TestChangeEvent.of(dest, 4, "c"));
        consumer.handleBatch(records, TestUtil.getCommitter());
        ResultSet rsR = getDatabendTableData("select * from public.debeziumcdc_customers_upsert where id = 1 AND __op= 'r'");
        Assertions.assertEquals(getResultSetRowCount(rsR), 1);
        ResultSet rsD = getDatabendTableData("select * from public.debeziumcdc_customers_upsert where id = 2 AND __op= 'd'");
        Assertions.assertEquals(getResultSetRowCount(rsD), 1);
        ResultSet rsU = getDatabendTableData("select * from public.debeziumcdc_customers_upsert where id = 3 AND __op= 'u'");
        Assertions.assertEquals(getResultSetRowCount(rsU), 1);
        ResultSet rsUName = getDatabendTableData("select * from public.debeziumcdc_customers_upsert where id = 3 AND first_name= 'UpdatednameV1'");
        Assertions.assertEquals(getResultSetRowCount(rsUName), 1);
    }

//    @AfterEach
//    public void clearData() throws SQLException, ClassNotFoundException {
////        ResultSet rs = select("delete from public.debeziumcdc_customers_upsert");
//        ResultSet rs = select("Drop database if exists public");
//    }

    public static class DatabendChangeConsumerUpsertTestProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();

            config.put("debezium.sink.databend.upsert", "true");
            config.put("debezium.sink.databend.upsert-keep-deletes", "true");
            return config;
        }
    }
}
