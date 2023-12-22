/**
 *
 */

package io.confluent.connect.s3.format;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.s3.util.S3ErrorUtils.throwConnectException;
import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;

public class CustomRecordWriterProviderImpl extends RecordViewSetter
    implements CustomRecordWriterProvider<S3SinkConnectorConfig> {
  private final RecordWriterProvider<S3SinkConnectorConfig> internalRecordWriterProvider;
  private final S3Storage s3Storage;

  public CustomRecordWriterProviderImpl(S3Storage storage,
      RecordWriterProvider<S3SinkConnectorConfig> internalRecordWriterProvider) {
    this.internalRecordWriterProvider = internalRecordWriterProvider;
    this.s3Storage = storage;
  }

  @Override
  public String getExtension() {
    return this.internalRecordWriterProvider.getExtension();
  }

  @Override
  public CustomRecordWriter getRecordWriter(
        S3SinkConnectorConfig configuration,
        String temporaryFilename) {
    return new RecordWriterWrapper(
        internalRecordWriterProvider.getRecordWriter(configuration, temporaryFilename),
        temporaryFilename
    );
  }

  class RecordWriterWrapper implements CustomRecordWriter {
    private final Logger log = LoggerFactory.getLogger(RecordWriterWrapper.class);

    private final RecordWriter internalRecordWriter;
    private final String internalRecordWriterFilename;

    RecordWriterWrapper(RecordWriter internalRecordWriter,
                        String internalRecordWriterFilename) {
      this.internalRecordWriter = internalRecordWriter;
      this.internalRecordWriterFilename = internalRecordWriterFilename;
    }

    @Override
    public void write(SinkRecord sinkRecord) {
      internalRecordWriter.write(sinkRecord);
    }

    @Override
    public void close() {
      internalRecordWriter.close();
    }

    @Override
    public void commit() {
      internalRecordWriter.commit();
    }

    @Override
    public void commit(String filename) {
      // let commit
      internalRecordWriter.commit();

      // 'move' the committed file to the "filename" parameter
      String sourceFilename = getAdjustedFilename(
          recordView,
          internalRecordWriterFilename,
          internalRecordWriterProvider.getExtension());
      String targetFilename = getAdjustedFilename(
          recordView,
          filename,
          internalRecordWriterProvider.getExtension());
      if (sourceFilename.equals(targetFilename)) {
        throwConnectException(new IllegalStateException(
            "Source filename is the same as target filename "
            + sourceFilename
        ));
      }
      s3Storage.copyObject(sourceFilename, targetFilename);
      log.info("Copy object from '{}' to target filename '{}'",
          sourceFilename, targetFilename);

      s3Storage.deleteObject(sourceFilename);
      log.info("Deleting object '{}'", sourceFilename);
    }
  }
}
