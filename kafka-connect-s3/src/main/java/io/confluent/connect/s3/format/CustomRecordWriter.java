/**
 *
 */

package io.confluent.connect.s3.format;

import io.confluent.connect.storage.format.RecordWriter;

public interface CustomRecordWriter extends RecordWriter {
  void commit(String filename);
}
