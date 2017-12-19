package com.github.animeshtrivedi.FileBench;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.net.URI;

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
    private ParquetFileReader parquetFileReader;

    private Path arrowPath;
    private Schema arrowSchema;
    private VectorSchemaRoot arrowVectorSchemaRoot;
    private ArrowFileWriter arrowFileWriter;
    private RootAllocator ra = null;

    public ParquetToArrow(){
        this.conf = new Configuration();
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.arrowPath = new Path("/arrowOutput/");
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
        setArrowFileWriter(convertParquetToArrowFileName(this.parqutFilePath));
    }

    private String convertParquetToArrowFileName(Path parquetNamePath){
        String oldsuffix = ".parquet";
        String newSuffix = ".arrow";
        String fileName = parquetNamePath.getName();
        if (!fileName.endsWith(oldsuffix)) {
            return fileName + newSuffix;
        }
        return fileName.substring(0, fileName.length() - oldsuffix.length()) + newSuffix;
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
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Int(32, true)), null));
                     break;
                 case INT64 :
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Int(64, true)), null));
                     break;
                 case DOUBLE :
                     childrenBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                     break;
                 case BINARY :
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

    private void setArrowFileWriter(String arrowFileName) throws Exception{
        String arrowFullPath = this.arrowPath.toUri().toString() + "/" + arrowFileName;
        System.out.println("Creating a file with name : " + arrowFullPath);
        // create the file stream on HDFS
        Path path = new Path(arrowFullPath);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        // default is to over-write
        FSDataOutputStream file = fs.create(new Path(path.toUri().getRawPath()));
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

        this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot,
                provider,
                new ArrowOutputStream(file));
    }

    public void process(){

    }
}
