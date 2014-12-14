/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.streams.s3;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This class writes to a temporary file on disk, then it uploads that temporary file whenever the file is closed.
 */
public class S3OutputStreamWrapper implements Flushable
{
    public interface S3OutputStreamWrapperCloseCallback {
        void completed();
        void error();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(S3OutputStreamWrapper.class);

    private final AmazonS3Client amazonS3Client;
    private final String bucketName;
    private final String path;
    private final String fileName;
    private final File file;
    private final OutputStreamWriter outputStream;
    private final Map<String, String> metaData;
    private boolean isClosed = false;

    /**
     * Create an OutputStream Wrapper
     * @param amazonS3Client
     * The Amazon S3 Transfer Manager for this session
     * @param bucketName
     * The Bucket Name you are wishing to write to.
     * @param path
     * The path where the object will live
     * @param fileName
     * The fileName you ware wishing to write.
     * @param metaData
     * Any meta data that is to be written along with the object
     * @throws IOException
     * If there is an issue creating the stream, this
     */
    public S3OutputStreamWrapper(AmazonS3Client amazonS3Client, String bucketName, String path, String fileName, Map<String, String> metaData) throws IOException {
        this.amazonS3Client = amazonS3Client;
        this.bucketName = bucketName;
        this.path = path;
        this.fileName = fileName;
        this.metaData = metaData != null ? metaData : new HashMap<String, String>();
        this.file = File.createTempFile("aws-s3-temp", ".tsv");
        this.file.deleteOnExit();
        this.outputStream = new OutputStreamWriter(new FileOutputStream(file));
    }

    public void write(int b) throws IOException {
        this.outputStream.write(b);
    }

    public void write(String str) throws IOException {
        this.outputStream.write(str);
    }

    public void flush() throws IOException {
        this.outputStream.flush();
    }

    /**
     * An asynchronous close. Upon close the file is written
     */
    public void close() {
        closeWithNotification(null);
    }

    public void closeWithNotification(final S3OutputStreamWrapperCloseCallback callback) {
        if(!isClosed)
        {
            try
            {
                this.addFile(new S3ProgressListener() {
                    @Override
                    public void onPersistableTransfer(PersistableTransfer persistableTransfer) {
                        LOGGER.info("persistableTransfer: {}", persistableTransfer.toString());
                    }

                    @Override
                    public void progressChanged(ProgressEvent progressEvent) {
                        if(progressEvent.getEventType().equals(ProgressEventType.TRANSFER_COMPLETED_EVENT)) {
                            LOGGER.info("File COMPLETED: {}", fileName);
                            if(!file.delete()) {
                                LOGGER.warn("Unable to delete temporary file: {}", file.getAbsolutePath());
                            }
                            if(callback != null)
                                callback.completed();
                        }
                    }
                });
            }
            catch(Exception e) {
                e.printStackTrace();
                LOGGER.warn("There was an error adding the temporaryFile to S3");
            }
            finally {
                // we are done here.
                this.isClosed = true;
            }
        }
    }

    private void addFile(S3ProgressListener s3ProgressListener) throws Exception {

        this.outputStream.flush();
        this.outputStream.close();

        final TransferManager transferManager = new TransferManager(this.amazonS3Client);

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, path + fileName, this.file);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setExpirationTime(DateTime.now().plusDays(365 * 3).toDate());
        metadata.addUserMetadata("writer", "org.apache.streams");
        for(String s : metaData.keySet())
            metadata.addUserMetadata(s, metaData.get(s));

        putObjectRequest.setMetadata(metadata);

        transferManager.upload(putObjectRequest, s3ProgressListener);

        LOGGER.info("AddFile Complete: {}", fileName);

    }


}
