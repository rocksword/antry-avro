package com.an.antry;

import org.apache.avro.Schema;

public class AvroUtilsTest {
    private static final String schemaDescription = "{ \n" + " \"namespace\": \"com.navteq.avro\", \n"
            + " \"name\": \"FacebookUser\", \n" + " \"type\": \"record\",\n" + " \"fields\": [\n"
            + " {\"name\": \"name\", \"type\": [\"string\", \"null\"] },\n"
            + " {\"name\": \"num_likes\", \"type\": \"int\"},\n" + " {\"name\": \"num_photos\", \"type\": \"int\"},\n"
            + " {\"name\": \"num_groups\", \"type\": \"int\"} ]\n" + "}";

    private static final String schemaDescriptionExt = " { \n" + " \"namespace\": \"com.navteq.avro\", \n"
            + " \"name\": \"FacebookSpecialUser\", \n" + " \"type\": \"record\",\n" + " \"fields\": [\n"
            + " {\"name\": \"user\", \"type\": com.navteq.avro.FacebookUser },\n"
            + " {\"name\": \"specialData\", \"type\": \"int\"} ]\n" + "}";

    public static void main(String[] args) {
        Schema desc = AvroUtils.parseSchema(schemaDescription);
        System.out.println(desc.toString(true));

        System.out.println("\n\n");
        Schema extended = AvroUtils.parseSchema(schemaDescriptionExt);
        System.out.println(extended.toString(true));
    }
}
