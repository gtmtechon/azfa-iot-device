package com.gtmtech.azf;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

// WaterBotState 엔티티와 동일한 구조의 POJO를 여기에 정의하거나 임포트합니다.
// 실제 프로젝트에서는 com.handson.gtm.iotmon.entity.WaterBotState 를 임포트하여 사용하세요.
// import com.handson.gtm.iotmon.entity.WaterBotState;

/**
 * WaterBot의 최신 상태를 조회하는 Azure Function.
 * HTTP GET 요청을 통해 모든 WaterBot의 현재 상태를 반환합니다.
 */
public class WaterBotStatusFunction {

    // JSON 직렬화/역직렬화를 위한 ObjectMapper 인스턴스
    private final ObjectMapper objectMapper;

    /**
     * WaterBotStatusFunction의 생성자.
     * ObjectMapper를 초기화하고 LocalDateTime 직렬화를 위한 모듈을 등록합니다.
     */
    public WaterBotStatusFunction() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // Java 8 날짜/시간 API 지원
    }

    /**
     * WaterBotState 엔티티와 동일한 구조를 가진 POJO 클래스.
     * 데이터베이스에서 조회한 결과를 매핑하는 데 사용됩니다.
     * 실제 프로젝트에서는 com.handson.gtm.iotmon.entity.WaterBotState를 임포트하여 사용하세요.
     */
    public static class WaterBotState {
        public String botId;
        public String botName;
        public String location;
        public String locationCooSys;
        public String status;
        public LocalDateTime lastUpdated;

        // 기본 생성자 (Jackson 역직렬화에 필요)
        public WaterBotState() {}

        // 모든 필드를 포함하는 생성자
        public WaterBotState(String botId, String botName, String location, String locationCooSys, String status, LocalDateTime lastUpdated) {
            this.botId = botId;
            this.botName = botName;
            this.location = location;
            this.locationCooSys = locationCooSys;
            this.status = status;
            this.lastUpdated = lastUpdated;
        }
    }


    /**
     * WaterBot의 최신 상태를 조회하는 HTTP 트리거 함수.
     * 클라이언트로부터 HTTP GET 요청을 받아 데이터베이스에서 모든 WaterBot의 최신 상태를 조회하여 반환합니다.
     *
     * @param request HTTP 요청 메시지 (경로: /api/waterbotstatus/latest)
     * @param context 함수 실행 컨텍스트 (로깅 등)
     * @return HTTP 응답 메시지 (성공 시 200 OK와 WaterBotState 목록 JSON, 오류 시 500 Internal Server Error)
     */
    @FunctionName("GetWaterBotLatestStatus") // Azure Portal에 표시될 함수 이름
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET}, // GET 메서드에 대한 HTTP 트리거
                authLevel = com.microsoft.azure.functions.annotation.AuthorizationLevel.FUNCTION, // 인증 수준 (FUNCTION, ANONYMOUS 등)
                route = "robots/status") // 이 함수의 API 경로 접미사
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger for GetWaterBotLatestStatus processed a request.");

        // Azure Function의 애플리케이션 설정에서 PostgreSQL 연결 정보를 가져옵니다.
        // 이 환경 변수들은 Azure Portal -> Function App -> 구성 -> 애플리케이션 설정에서 설정해야 합니다.
        String dbHost = System.getenv("DB_HOST");
        String dbPort = System.getenv("DB_PORT");
        String dbName = System.getenv("DB_NAME");
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");
         String jdbcUrl = System.getenv("JDBC_URL");

        // JDBC 연결 URL 구성
        //String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);
        

        List<WaterBotState> latestStates = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
            // waterbot_status 테이블에서 모든 로봇의 최신 상태를 조회합니다.
            // botId가 PRIMARY KEY이므로, 각 botId에 대한 최신 상태만 존재한다고 가정합니다.
            String sql = "SELECT botid, botname, location, locationcoosys, status, lastupdated FROM waterbot_status";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    // ResultSet에서 데이터를 읽어 WaterBotState 객체로 매핑합니다.
                    latestStates.add(new WaterBotState(
                        rs.getString("botid"),
                        rs.getString("botname"),
                        rs.getString("location"),
                        rs.getString("locationcoosys"),
                        rs.getString("status"),
                        rs.getTimestamp("lastupdated").toLocalDateTime() // Timestamp를 LocalDateTime으로 변환
                    ));
                }
            }

            // 조회된 WaterBotState 목록을 JSON으로 직렬화하여 200 OK 응답을 반환합니다.
            return request.createResponseBuilder(HttpStatus.OK)
                          .header("Content-Type", "application/json")
                          .body(objectMapper.writeValueAsString(latestStates))
                          .build();

        } catch (SQLException e) {
            // 데이터베이스 연결 또는 쿼리 중 오류 발생 시 500 Internal Server Error 반환
            context.getLogger().severe("Database error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Database error: " + e.getMessage()).build();
        } catch (Exception e) {
            // 기타 예상치 못한 오류 발생 시 500 Internal Server Error 반환
            context.getLogger().severe("An unexpected error occurred: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred: " + e.getMessage()).build();
        }
    }
}
