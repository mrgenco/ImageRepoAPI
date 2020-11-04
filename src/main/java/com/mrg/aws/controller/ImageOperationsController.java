package com.mrg.aws.controller;

import com.mrg.aws.service.AWSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping(value= "/image")
public class ImageOperationsController {

    private AWSService service;

    @Autowired
    ImageOperationsController(AWSService service){
        this.service = service;
    }

    @PostMapping(value= "/upload")
    public ResponseEntity<String> uploadFile(@RequestPart(value= "image") final MultipartFile multipartFile,
                                             @RequestPart(value= "description") final String description) {
        try{
            service.uploadFile(multipartFile, description);
            final String response = "[" + multipartFile.getOriginalFilename() + "] uploaded successfully.";
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        }catch(Exception ex){
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // TODO : Search endpoint will be added




}