package com.github.animeshtrivedi.FileBench;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.FileMetadata;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.arrow.vector.types.FloatingPointPrecision.*;
import static org.apache.parquet.schema.PrimitiveType.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Created by atr on 19.12.17.
 */
public class ParquetToArrow {
    private Configuration conf;
    private Path parqutFilePath;
    private MessageType parquetSchema;
    private Schema arrowSchema;
    private ParquetFileReader parquetFileReader;

    public ParquetToArrow(){
        this.conf = new Configuration();
    }

    public void setParquetInputFile(String parquetFile) throws Exception {
        this.parqutFilePath = new Path(parquetFile);
        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf,
                this.parqutFilePath,
                ParquetMetadataConverter.NO_FILTER);

        FileMetaData mdata = readFooter.getFileMetaData();
        this.parquetSchema = mdata.getSchema();
        this.parquetFileReader = new ParquetFileReader(conf,
                mdata,
                this.parqutFilePath,
                readFooter.getBlocks(),
                this.parquetSchema.getColumns());
        makeArrowSchema();
    }

    private void makeArrowSchema() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        for(ColumnDescriptor col: this.parquetSchema.getColumns()){
            sb.setLength(0);
            String[] p = col.getPath();
            for(String px: p)
                sb.append(px);
             switch (col.getType()) {
                 case INT32 :
                     System.out.println("ArrowSchema: INT: " + sb.toString() + " added");
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Int(32, true)), null));
                     break;
                 case INT64 :
                     System.out.println("ArrowSchema: LONG: " + sb.toString() + " added");
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Int(64, true)), null));
                     break;
                 case DOUBLE :
                     System.out.println("ArrowSchema: DOUBLE: " + sb.toString() + " added");
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                     break;
                 case BINARY :
                     System.out.println("ArrowSchema: BINARY: " + sb.toString() + " added");
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Binary()), null));
                     break;
                     // has float
                 //case FLOAT:
                 default : throw new Exception(" NYI " + col.getType());
            }
        }
        this.arrowSchema = new Schema(childrenBuilder.build(), null);
        System.out.println("Arrow Schema is " + this.arrowSchema.toString());
    }

    public void process(){

    }

}
