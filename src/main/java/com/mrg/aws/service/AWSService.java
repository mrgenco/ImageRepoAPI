package com.mrg.aws.service;

import org.springframework.web.multipart.MultipartFile;

public interface AWSService {

	void uploadFile(MultipartFile multipartFile, String description) throws Exception;
}
