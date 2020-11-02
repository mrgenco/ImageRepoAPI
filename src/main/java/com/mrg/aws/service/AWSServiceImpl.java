package com.mrg.aws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
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

    @Autowired
    private S3Client amazonS3;

    @Autowired
    private DynamoDbClient amazonDynamoDB;

    @Value("${aws.s3.bucket}")
    private String bucketName;

    @Override
    // @Async annotation ensures that the method is executed in a different background thread
    // but not consume the main thread.
    @Async
    public void uploadFile(final MultipartFile multipartFile, final String description) throws Exception {
        LOGGER.info("File upload in progress.");
        try {
            final File file = convertMultiPartFileToFile(multipartFile);
            uploadFileToS3Bucket(bucketName, file);
            LOGGER.info("File upload is completed.");
            updateDynamoDBTable(multipartFile.getOriginalFilename(), multipartFile.getSize(), description);
            LOGGER.info("DynamoDB table is updated.");
            file.delete();    // To remove the file locally created in the project folder.
        } catch (Exception ex) {
            LOGGER.info("File upload is failed.");
            LOGGER.error("Error= {} while uploading file.", ex.getMessage());
            throw ex;
        }
    }

    private void updateDynamoDBTable(String originalFilename, long size, String description) {
        HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
        // Add all content to the table
        itemValues.put("fileName", AttributeValue.builder().s(originalFilename).build());
        itemValues.put("size", AttributeValue.builder().s(String.valueOf(size)).build());
        itemValues.put("description", AttributeValue.builder().s(description).build());
        // Create a PutItemRequest object
        PutItemRequest request = PutItemRequest.builder()
                .tableName("mrgtable")
                .item(itemValues)
                .build();
        try {
            amazonDynamoDB.putItem(request);
            System.out.println("mrgtable" + " was successfully updated");
        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The table \"%s\" can't be found.\n", "mrgtable");
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private File convertMultiPartFileToFile(final MultipartFile multipartFile) throws Exception {
        final File file = new File(multipartFile.getOriginalFilename());

        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(multipartFile.getBytes());
        } catch (final IOException ex) {
            LOGGER.error("Error converting the multi-part file to file= ", ex.getMessage());
            throw ex;
        }
        return file;
    }

    private void uploadFileToS3Bucket(final String bucketName, final File file) {
        final String uniqueFileName = LocalDateTime.now() + "_" + file.getName();
        LOGGER.info("Uploading file with name= " + uniqueFileName);
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(uniqueFileName)
                .build();
        amazonS3.putObject(objectRequest, RequestBody.fromFile(file));
    }
}
