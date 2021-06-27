package cmy.test.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Writer {

    public static String schemaFilePath = Writer.class.getClassLoader().getResource("file.schema").getPath();;

    public static void main(String[] args) throws IOException {
        List<List<String>> columns = getDataForFile();
        MessageType schema = getSchemaForParquetFile();
        CustomParquetWriter writer = getParquetWriter(schema);

        for (List<String> column : columns) {
            System.out.println("Writing line: " + column.toArray());
            writer.write(column);
        }
        System.out.println("Finished writing Parquet file.");

        writer.close();
    }

    private static CustomParquetWriter getParquetWriter(MessageType schema) throws IOException {
        String outputFilePath = "./" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(path, schema, false, CompressionCodecName.SNAPPY);
    }

    private static MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File(schemaFilePath);
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    private static List<List<String>> getDataForFile() {
        List<List<String>> data = new ArrayList<List<String>>();

        List<String> parquetFileItem1 = new ArrayList<String>();
        parquetFileItem1.add("1");
        parquetFileItem1.add("4206.9");
        //parquetFileItem1.add("true");

        List<String> parquetFileItem2 = new ArrayList<String>();
        parquetFileItem2.add("2");
        parquetFileItem2.add("4206.8");
        // parquetFileItem2.add("false");

        data.add(parquetFileItem1);
        data.add(parquetFileItem2);

        return data;
    }
}
