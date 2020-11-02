package com.mrg.aws.service;

import com.mrg.aws.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;

@Service
public class AWSServiceImpl implements AWSService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSServiceImpl.class);
    private S3Client amazonS3;
    private DynamoDbClient amazonDynamoDB;

    @Value("${aws.s3.bucket}")
    private String bucketName;
    @Value("${aws.dynamodb.tablename}")
    private String tableName;

    @Autowired
    AWSServiceImpl(S3Client amazonS3, DynamoDbClient amazonDynamoDB){
        this.amazonS3 = amazonS3;
        this.amazonDynamoDB = amazonDynamoDB;
    }

    @Override
    @Async
    public void uploadFile(final MultipartFile multipartFile, final String description) throws Exception {
        LOGGER.info("File upload in progress.");
        // Unique File ID for S3 and DynamoDB records
        final String uniqueFileId = LocalDateTime.now() + "_" + multipartFile.getOriginalFilename();
        // DynamoDB operation
        addFileToDynamoDBTable(uniqueFileId, multipartFile, description);
        // S3 operation
        uploadFileToS3Bucket(uniqueFileId, multipartFile, bucketName);
    }

    private void addFileToDynamoDBTable(String uniqueId, MultipartFile multipartFile, String description) {
        try {
            LOGGER.info("Adding DynamoDB record with name= " + uniqueId);
            HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
            itemValues.put("id", AttributeValue.builder().s(uniqueId).build());
            itemValues.put("fileName", AttributeValue.builder().s(multipartFile.getOriginalFilename()).build());
            itemValues.put("fileType", AttributeValue.builder().s(multipartFile.getContentType()).build());
            itemValues.put("size", AttributeValue.builder().s(String.valueOf(multipartFile.getSize())).build());
            itemValues.put("description", AttributeValue.builder().s(description).build());
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(itemValues)
                    .build();
            amazonDynamoDB.putItem(request);
            LOGGER.info("File upload DynamoDB is completed.");
        } catch (Exception ex) {
            LOGGER.error("Error= {} while adding new file to DynamoDB.", ex.getMessage());
            throw ex;
        }
    }

    private void uploadFileToS3Bucket(final String uniqueFileId, final MultipartFile multipartFile,  final String bucketName) throws Exception {
        try {
            final File file = FileUtils.convertMultiPartFileToFile(multipartFile);
            LOGGER.info("Uploading file with name= " + uniqueFileId);
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(uniqueFileId)
                    .build();
            amazonS3.putObject(objectRequest, RequestBody.fromFile(file));
            LOGGER.info("File upload S3 is completed.");
            file.delete();
        }catch (Exception ex) {
            LOGGER.error("Error= {} while uploading file to S3.", ex.getMessage());
            // Rollback operation
            rollBackFromDynamoDBTable(uniqueFileId);
            throw ex;
        }
    }

    private void rollBackFromDynamoDBTable(String uniqueFileId) {
        try {
            LOGGER.info("Rollback started for DynamoDB record.");
            HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
            keyToGet.put("id", AttributeValue.builder()
                    .s(uniqueFileId)
                    .build());
            DeleteItemRequest deleteReq = DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(keyToGet)
                    .build();
            amazonDynamoDB.deleteItem(deleteReq);
        } catch (Exception ex) {
            LOGGER.error("Rollback operation failed. Error = {}", ex.getMessage());
        }
    }
}
