package com.mrg.aws.service;

import org.springframework.web.multipart.MultipartFile;

import java.util.UUID;

public interface AWSService {

	void uploadFile(MultipartFile multipartFile, String description, String tags) throws Exception;
	byte[] download(UUID imageId);
}
