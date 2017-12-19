package com.github.animeshtrivedi.FileBench;

import com.github.animeshtrivedi.FileBench.helper.DumpGroupConverter;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
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
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.apache.arrow.vector.types.FloatingPointPrecision.*;
import static org.apache.parquet.schema.OriginalType.*;
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
    private ParquetMetadata parquetFooter;

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
        this.parquetFooter = ParquetFileReader.readFooter(conf,
                this.parqutFilePath,
                ParquetMetadataConverter.NO_FILTER);

        FileMetaData mdata = this.parquetFooter.getFileMetaData();
        this.parquetSchema = mdata.getSchema();
        this.parquetFileReader = new ParquetFileReader(conf,
                mdata,
                this.parqutFilePath,
                this.parquetFooter.getBlocks(),
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

    public void process() throws Exception {
        PageReadStore pageReadStore = null;
        List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        int size = colDesc.size();
        DumpGroupConverter conv = new DumpGroupConverter();
        this.arrowFileWriter.start();

        pageReadStore = parquetFileReader.readNextRowGroup();
        while (pageReadStore != null) {
            ColumnReadStoreImpl colReader = new ColumnReadStoreImpl(pageReadStore, conv,
                    this.parquetSchema, this.parquetFooter.getFileMetaData().getCreatedBy());
            int i = 0;
            while (i < size){
                ColumnDescriptor col = colDesc.get(i);
                switch(col.getType()) {
                    case INT32: writeIntColumn(colReader, col, fieldVectors.get(i)); break;
                    case INT64: writeLongColumn(colReader, col, fieldVectors.get(i)); break;
                    case DOUBLE: writeDoubleColumn(colReader, col, fieldVectors.get(i)); break;
                    case BINARY: writeBinaryColumn(colReader, col, fieldVectors.get(i)); break;
                    default : throw new Exception(" NYI " + col.getType());
                }
                i+=1;
            }
            pageReadStore = parquetFileReader.readNextRowGroup();
            this.arrowFileWriter.writeBatch();
        }
        this.arrowFileWriter.end();
        this.arrowFileWriter.close();
    }

    private long writeIntColumn(ColumnReadStoreImpl crstore,
                                org.apache.parquet.column.ColumnDescriptor column,
                                FieldVector fieldVector) throws Exception {
        int dmax = column.getMaxDefinitionLevel();
        ColumnReader creader = crstore.getColumnReader(column);
        long rows = creader.getTotalValueCount();
        if(rows > Integer.MAX_VALUE)
            throw new Exception(" More than Integer.MAX_VALUE is not supported " + rows);

        NullableIntVector intVector = (NullableIntVector) fieldVector;
        NullableIntVector.Mutator mutator = intVector.getMutator();
        intVector.setInitialCapacity((int) rows);
        intVector.allocateNew();

        for(int i = 0; i < (int) rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                int x = creader.getInteger();
                // do something with x
                mutator.setIndexDefined(i);
                // there is setSafe too - what does that mean? TODO:
                mutator.set(i, 1, x);
            } else {
                mutator.setNull(i);
            }
            creader.consume();
        }
        mutator.setValueCount((int) rows);
        return rows;
    }

    private long writeLongColumn(ColumnReadStoreImpl crstore,
                                org.apache.parquet.column.ColumnDescriptor column,
                                FieldVector fieldVector) throws Exception {
        int dmax = column.getMaxDefinitionLevel();
        ColumnReader creader = crstore.getColumnReader(column);
        long rows = creader.getTotalValueCount();
        if(rows > Integer.MAX_VALUE)
            throw new Exception(" More than Integer.MAX_VALUE is not supported " + rows);

        NullableBigIntVector bigIntVector = (NullableBigIntVector) fieldVector;
        NullableBigIntVector.Mutator mutator = bigIntVector.getMutator();
        bigIntVector.setInitialCapacity((int) rows);
        bigIntVector.allocateNew();

        for(int i = 0; i < (int) rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                long x = creader.getLong();
                // do something with x
                mutator.setIndexDefined(i);
                // there is setSafe too - what does that mean? TODO:
                mutator.set(i, 1, x);
            } else {
                mutator.setNull(i);
            }
            creader.consume();
        }
        mutator.setValueCount((int) rows);
        return rows;
    }

    private long writeDoubleColumn(ColumnReadStoreImpl crstore,
                                 org.apache.parquet.column.ColumnDescriptor column,
                                 FieldVector fieldVector) throws Exception {
        int dmax = column.getMaxDefinitionLevel();
        ColumnReader creader = crstore.getColumnReader(column);
        long rows = creader.getTotalValueCount();
        if(rows > Integer.MAX_VALUE)
            throw new Exception(" More than Integer.MAX_VALUE is not supported " + rows);

        NullableFloat8Vector float8Vector  = (NullableFloat8Vector ) fieldVector;
        NullableFloat8Vector.Mutator mutator = float8Vector.getMutator();
        float8Vector.setInitialCapacity((int) rows);
        float8Vector.allocateNew();

        for(int i = 0; i < (int) rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                double x = creader.getDouble();
                // do something with x
                mutator.setIndexDefined(i);
                // there is setSafe too - what does that mean? TODO:
                mutator.set(i, 1, x);
            } else {
                mutator.setNull(i);
            }
            creader.consume();
        }
        mutator.setValueCount((int) rows);
        return rows;
    }

    private long writeBinaryColumn(ColumnReadStoreImpl crstore,
                                   org.apache.parquet.column.ColumnDescriptor column,
                                   FieldVector fieldVector) throws Exception {
        int dmax = column.getMaxDefinitionLevel();
        ColumnReader creader = crstore.getColumnReader(column);
        long rows = creader.getTotalValueCount();
        if(rows > Integer.MAX_VALUE)
            throw new Exception(" More than Integer.MAX_VALUE is not supported " + rows);

        NullableVarBinaryVector varBinaryVector  = (NullableVarBinaryVector) fieldVector;
        NullableVarBinaryVector.Mutator mutator = varBinaryVector.getMutator();
        varBinaryVector.setInitialCapacity((int) rows);
        varBinaryVector.allocateNew();
        for(int i = 0; i < (int) rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                byte[] data = creader.getBinary().getBytes();
                // do something with x
                mutator.setIndexDefined(i);
                mutator.setValueLengthSafe(i, data.length);
                mutator.set(i, data);
            } else {
                mutator.setNull(i);
            }
            creader.consume();
        }
        mutator.setValueCount((int) rows);
        return rows;
    }
}
