package com.example.catalogservice.messagequeue;

import com.example.catalogservice.entity.CatalogEntity;
import com.example.catalogservice.repository.CatalogRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * 실제 리스너를 이용하여 데이터를 가지고 오며 그 데이터를 가지고 DB 업데이트
 */
@Service
@Slf4j
public class KafkaConsumer {

    CatalogRepository repository;

    @Autowired
    public KafkaConsumer(CatalogRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "example-catalog-topic")
    public void updateQty(String kafkaMessage) {
        log.info("kafka Message : ->"+kafkaMessage);

        Map<Object, Object> map = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            // KafkaMessage 가 String 으로 들어 오지만 해당 데이터를 JSON 타입으로 변경하여 사용
            map = mapper.readValue(kafkaMessage, new TypeReference<Map<Object, Object>>() {});


        }catch(JsonProcessingException e){
            e.printStackTrace();
        }
        //productId 로 엔티티 조회
        CatalogEntity entity = repository.findByProductId((String) map.get("productId"));

        if (entity != null){
            // 조회된 수량에서 넘어온 데이터의 수량을 차감하여 업데이트
            entity.setStock(entity.getStock() - (Integer) map.get("qty"));
            repository.save(entity);
        }
    }
}
