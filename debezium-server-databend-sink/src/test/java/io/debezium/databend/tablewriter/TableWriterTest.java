package io.debezium.databend.tablewriter;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static io.debezium.server.databend.tablewriter.BaseTableWriter.replaceFirstWordAfterTable;

public class TableWriterTest {
    @Test
    public void testFirstWordAfterTable() throws Exception {
        String statement = "alter table products add column a int";
        String newStatement = replaceFirstWordAfterTable(statement, "newTable");
        System.out.println(newStatement);
        Assert.assertEquals(newStatement, "alter table newTable add column a int");

        statement = "alter table products drop column a";
        newStatement = replaceFirstWordAfterTable(statement, "yyy");
        System.out.println(newStatement);
        Assert.assertEquals(newStatement, "alter table yyy drop column a");
    }
}
