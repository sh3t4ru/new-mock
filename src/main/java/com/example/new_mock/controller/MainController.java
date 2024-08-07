package com.example.new_mock.controller;

import com.example.new_mock.model.RequestDTO;
import com.example.new_mock.model.ResponseDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Random;

@Slf4j
@RestController
public class MainController {
    private ObjectMapper mapper = new ObjectMapper();

    @PostMapping(
            value = "/info/postBalances",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Object postBalances(@RequestBody RequestDTO requestDTO) {
        try {
            String clientId = requestDTO.getClientId();
            char firstDigit = clientId.charAt(0);
            ResponseDTO responseDTO = new ResponseDTO();
            BigDecimal maxLimit;
            if (firstDigit == '8') {
                maxLimit = new BigDecimal(2000);
                responseDTO.setCurrency("US");
            } else if (firstDigit == '9') {
                maxLimit = new BigDecimal(1000);
                responseDTO.setCurrency("EU");
            } else {
                maxLimit = new BigDecimal(10000);
                responseDTO.setCurrency("RUB");
            }
            BigDecimal randomBalance = BigDecimal.valueOf(new Random().nextDouble()).multiply(maxLimit);
            randomBalance = randomBalance.setScale(2, BigDecimal.ROUND_HALF_UP);
            responseDTO.setClientId(requestDTO.getClientId());
            responseDTO.setRqUID(requestDTO.getRqUID());
            responseDTO.setAccount(requestDTO.getAccount());
            responseDTO.setBalance(randomBalance);
            responseDTO.setMaxLimit(maxLimit);

            log.error("********** RequestDTO **********" +
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestDTO));
            log.error("********** ResponseDTO **********" +
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseDTO));

            return responseDTO;

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }
}
