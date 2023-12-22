/**
 *
 */

package io.confluent.connect.s3.format;

public interface CustomRecordWriterProvider<C> {
  String getExtension();

  CustomRecordWriter getRecordWriter(C configuration, String temporaryFilename);
}
