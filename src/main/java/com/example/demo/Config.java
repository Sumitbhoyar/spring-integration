package com.example.demo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.jdbc.JdbcMessageHandler;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.messaging.MessageHandler;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Map;

@Configuration
public class Config {
    @Bean
    @InboundChannelAdapter(value = "input", poller = @Poller(fixedDelay="30000"))
    public MessageSource<?> storedProc(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcPollingChannelAdapter(dataSource, "SELECT * FROM Items WHERE INVENTORY_STATUS = 0");
    }

    @Bean
    @ServiceActivator(inputChannel = "input")
    public MessageHandler jdbcMessageHandler(@Qualifier("dataSource") DataSource dataSource) {
        JdbcMessageHandler jdbcMessageHandler = new JdbcMessageHandler(dataSource,
                "MERGE INTO Items2 KEY (ITEM_ID) VALUES (?, ?, ?)");
        jdbcMessageHandler.setPreparedStatementSetter((ps, m) -> {
            ArrayList<Map<String, ?>> objects = (ArrayList<Map<String, ?>>)m.getPayload();
            for (Map<String, ?> map: objects){
                ps.setString(1, (String) map.get("ITEM_ID"));
                ps.setString(2, (String) map.get("DESCRIPTION"));
                ps.setInt(3, (Integer) map.get("INVENTORY_STATUS"));
                ps.addBatch();
            }
            ps.executeBatch();
        });
        return jdbcMessageHandler;
    }
}
