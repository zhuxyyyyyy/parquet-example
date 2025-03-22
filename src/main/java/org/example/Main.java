package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;


public class Main {
    public static void writeParquet() throws Exception {
        // 定义 Parquet 的 schema
        String schemaString = "message example { " +
                "required binary name (UTF8); " +
                "required int32 age; " +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(schemaString);

        // 配置 Hadoop 配置和 GroupWriteSupport
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        GroupWriteSupport.setSchema(schema, conf);

        // 设置输出文件路径
        Path path = new Path("example.parquet");

        // 创建 ParquetWriter
        ParquetWriter<Group> writer = new ParquetWriter<>(
                path,
                conf,
                new GroupWriteSupport()
        );

        // 使用 SimpleGroupFactory 创建数据记录
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("name", "Alice")
                .append("age", 30);

        // 写入记录并关闭 writer
        writer.write(group);
        writer.close();
    }

    public static void readParquet() throws Exception {
        Path path = new Path("example.parquet");

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();

        GenericRecord record;
        while ((record = reader.read()) != null) {
            System.out.println(record);
        }

        reader.close();
    }

    public static void main(String[] args) throws Exception {
        readParquet();
    }
}