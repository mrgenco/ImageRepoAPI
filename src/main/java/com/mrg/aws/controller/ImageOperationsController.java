package com.mrg.aws.controller;

import com.mrg.aws.service.AWSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.UUID;

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
                                             @RequestPart(value= "description") final String description,
                                             @RequestPart(value= "tags") final String tags) {
        try{
            service.uploadFile(multipartFile, description, tags);
            final String response = "[" + multipartFile.getOriginalFilename() + "] uploaded successfully.";
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        }catch(Exception ex){
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/download/{imageId}")
    public byte[] downloadUserProfileImage(@PathVariable("imageId") UUID imageId) {
       return service.download(imageId);
    }


    @GetMapping(value= "/search")
    public ResponseEntity<String> searchFile(){
        return new ResponseEntity<>(HttpStatus.FOUND);
    }

}