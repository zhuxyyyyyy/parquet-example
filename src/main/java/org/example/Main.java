package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.fs.FileSystem;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    public static void writeParquet() throws Exception {
        // 定义 Parquet 的 schema
        String schemaString = "message example { " +
                "required binary name (UTF8); " +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(schemaString);

        // 配置 Hadoop 配置和 GroupWriteSupport
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        GroupWriteSupport.setSchema(schema, conf);

        // 设置输出文件路径
        Path path = new Path("example.parquet");

        // 如果存在，则删除
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true); // 第二个参数 true 表示递归删除
        }

        // 创建 ParquetWriter
        ParquetWriter<Group> writer = new ParquetWriter<>(
                path,
                conf,
                new GroupWriteSupport()
        );


        for (int i = 0; i < 20; i++) {
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            String value = String.valueOf(i);
            Group group = groupFactory.newGroup()
                    .append("name", value);
            writer.write(group);
        }

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
        BasicConfigurator.configure();
        log.setLevel(Level.INFO);
        log.info("Starting parquet file system");

        writeParquet();
        readParquet();
    }
}