package com.gtmtech.azf;



import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule; // LocalDateTime 등을 위한 모듈

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class CurrentDeviceStateFunction {


    private final ObjectMapper objectMapper; // JSON 직렬화/역직렬화

    public CurrentDeviceStateFunction() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // 날짜/시간 타입 지원
    }

    // 디바이스 정보를 담을 간단한 POJO (스프링 부트의 Device 엔티티와 유사)
    // 실제 Device 엔티티의 필드에 맞게 조정하세요.
    public static class CurrentDeviceState{
        public String id;
        public String name;
        public String location;
        public double temperature;
        public java.time.LocalDateTime lastUpdated; // java.time 패키지 사용

        // Lombok을 사용하지 않는 경우, 기본 생성자와 getter/setter 필요
        public  CurrentDeviceState() {}
        public  CurrentDeviceState(String id, String name, String location, double temperature, java.time.LocalDateTime lastUpdated) {
            this.id = id;
            this.name = name;
            this.location = location;
            this.temperature = temperature;
            this.lastUpdated = lastUpdated;
        }
    }

    /**
     * Handles HTTP requests for /api/devices or /api/devices/{id}
     *
     * 프런트엔드에서 변경 없이 호출하기 위해 기존 스프링 부트 API 경로를 따릅니다.
     * http://<your-function-app-name>.azurewebsites.net/api/devices
     * http://<your-function-app-name>.azurewebsites.net/api/devices/{id}
     */
    @com.microsoft.azure.functions.annotation.FunctionName("CurrentStateApiFunction")
    public HttpResponseMessage run(
            @com.microsoft.azure.functions.annotation.HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE},
                authLevel = com.microsoft.azure.functions.annotation.AuthorizationLevel.FUNCTION, // 또는 ANONYMOUS
                route = "api/devices/{id?}") // {id?}는 ID가 선택 사항임을 의미
            HttpRequestMessage<Optional<String>> request,
            @com.microsoft.azure.functions.annotation.BindingName("id") String id, // 경로에서 ID 추출
            final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger processed a request.");

        // PostgreSQL 연결 정보 (Azure Function의 애플리케이션 설정에서 가져오는 것이 모범 사례)
        // Azure Portal Function App -> 구성 -> 애플리케이션 설정에서 추가:
        // DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
        String dbHost = System.getenv("DB_HOST");
        String dbPort = System.getenv("DB_PORT");
        String dbName = System.getenv("DB_NAME");
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);

        // HTTP 메서드 및 경로에 따른 로직 분기
        HttpMethod method = request.getHttpMethod();
        String path = request.getUri().getPath(); // 요청 경로 (예: /api/devices/123)
        String cleanedPath = path.substring(path.indexOf("/api/devices")); // /api/devices/123 -> /api/devices/123

        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
            if (HttpMethod.GET.equals(method)) {
                if (id != null) {
                    // GET /api/devices/{id}
                    return getDeviceById(conn, id, request);
                } else {
                    // GET /api/devices
                    return getAllDevices(conn, request);
                }
            } else if (HttpMethod.POST.equals(method) && id == null) {
                // POST /api/devices
                return createDevice(conn, request);
            } else if (HttpMethod.PUT.equals(method) && id != null) {
                // PUT /api/devices/{id}
                return updateDevice(conn, id, request);
            } else if (HttpMethod.DELETE.equals(method) && id != null) {
                // DELETE /api/devices/{id}
                return deleteDevice(conn, id, request);
            } else {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Unsupported HTTP method or path.").build();
            }
        } catch (SQLException e) {
            context.getLogger().severe("Database error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Database error: " + e.getMessage()).build();
        } catch (Exception e) {
            context.getLogger().severe("An unexpected error occurred: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred: " + e.getMessage()).build();
        }
    }

    private HttpResponseMessage getAllDevices(Connection conn, HttpRequestMessage<Optional<String>> request) throws SQLException {
        List<CurrentDeviceState> devices = new ArrayList<>();
        String sql = "SELECT id, name, location, temperature, last_updated FROM devices";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                CurrentDeviceState device = new CurrentDeviceState(
                    rs.getString("id"),
                    rs.getString("name"),
                    rs.getString("location"),
                    rs.getDouble("temperature"),
                    rs.getTimestamp("last_updated").toLocalDateTime()
                );
                devices.add(device);
            }
        }
        try {
            return request.createResponseBuilder(HttpStatus.OK)
                          .header("Content-Type", "application/json")
                          .body(objectMapper.writeValueAsString(devices))
                          .build();
        } catch (Exception e) {
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error serializing response.").build();
        }
    }

    private HttpResponseMessage getDeviceById(Connection conn, String id, HttpRequestMessage<Optional<String>> request) throws SQLException {
        String sql = "SELECT id, name, location, temperature, last_updated FROM devices WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    CurrentDeviceState device = new CurrentDeviceState(
                        rs.getString("id"),
                        rs.getString("name"),
                        rs.getString("location"),
                        rs.getDouble("temperature"),
                        rs.getTimestamp("last_updated").toLocalDateTime()
                    );
                    return request.createResponseBuilder(HttpStatus.OK)
                                  .header("Content-Type", "application/json")
                                  .body(objectMapper.writeValueAsString(device))
                                  .build();
                } else {
                    return request.createResponseBuilder(HttpStatus.NOT_FOUND).body("Device not found with ID: " + id).build();
                }
            }
        } catch (Exception e) {
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error serializing response or database error: " + e.getMessage()).build();
        }
    }

    /**
     * Handles the creation of a new Device record in the database.
     * <p>
     * Parses the incoming HTTP request body to construct a {@link Device} object.
     * If the device ID is not provided, a new UUID is generated.
     * The device's last updated timestamp is set to the current time.
     * Attempts to insert the new device into the "devices" table.
     * </p>
     *
     * @param conn    The active SQL database connection.
     * @param request The HTTP request message containing the device data in JSON format.
     *     sample request body:
     * <pre>
     * {
     *     "name": "Temperature Sensor",
     *     "location": "Warehouse A",
     *     "temperature": 23.5  // 예시 온도 값 
    * * <p>{
        "id": "device-001",
        "name": "Temperature Sensor",
        "location": "Warehouse A",
        "temperature": 23.5,
        "lastUpdated": "2024-06-10T15:30:00"
    }
     * @return        An {@link HttpResponseMessage} indicating the result of the operation:
     *                <ul>
     *                  <li>{@code 201 Created} with the created device in JSON if successful.</li>
     *                  <li>{@code 400 Bad Request} if the request body is invalid or missing.</li>
     *                  <li>{@code 500 Internal Server Error} if the device could not be created.</li>
     *                </ul>
     * @throws SQLException If a database access error occurs.
     */


    private HttpResponseMessage createDevice(Connection conn, HttpRequestMessage<Optional<String>> request) throws SQLException {
        try {
            String requestBody = request.getBody().orElse("");
            if (requestBody.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Request body is empty.").build();
            }

            CurrentDeviceState newDevice = objectMapper.readValue(requestBody, CurrentDeviceState.class);
            if (newDevice.id == null || newDevice.id.trim().isEmpty()) {
                newDevice.id = java.util.UUID.randomUUID().toString();
            }
            newDevice.lastUpdated = java.time.LocalDateTime.now();

            String sql = "INSERT INTO devices (id, name, location, temperature, last_updated) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, newDevice.id);
                pstmt.setString(2, newDevice.name);
                pstmt.setString(3, newDevice.location);
                pstmt.setDouble(4, newDevice.temperature);
                pstmt.setTimestamp(5, Timestamp.valueOf(newDevice.lastUpdated));
                int affectedRows = pstmt.executeUpdate();
                if (affectedRows > 0) {
                    return request.createResponseBuilder(HttpStatus.CREATED)
                                  .header("Content-Type", "application/json")
                                  .body(objectMapper.writeValueAsString(newDevice))
                                  .build();
                } else {
                    return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to create device.").build();
                }
            }
        } catch (Exception e) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Invalid request body or database error: " + e.getMessage()).build();
        }
    }

    private HttpResponseMessage updateDevice(Connection conn, String id, HttpRequestMessage<Optional<String>> request) throws SQLException {
        try {
            String requestBody = request.getBody().orElse("");
            if (requestBody.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Request body is empty.").build();
            }

            CurrentDeviceState updatedDevice = objectMapper.readValue(requestBody, CurrentDeviceState.class);
            updatedDevice.id = id; // 경로의 ID를 사용
            updatedDevice.lastUpdated = java.time.LocalDateTime.now();

            String sql = "UPDATE devices SET name = ?, location = ?, temperature = ?, last_updated = ? WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, updatedDevice.name);
                pstmt.setString(2, updatedDevice.location);
                pstmt.setDouble(3, updatedDevice.temperature);
                pstmt.setTimestamp(4, Timestamp.valueOf(updatedDevice.lastUpdated));
                pstmt.setString(5, updatedDevice.id);
                int affectedRows = pstmt.executeUpdate();
                if (affectedRows > 0) {
                    return request.createResponseBuilder(HttpStatus.OK)
                                  .header("Content-Type", "application/json")
                                  .body(objectMapper.writeValueAsString(updatedDevice))
                                  .build();
                } else {
                    return request.createResponseBuilder(HttpStatus.NOT_FOUND).body("Device not found with ID: " + id).build();
                }
            }
        } catch (Exception e) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Invalid request body or database error: " + e.getMessage()).build();
        }
    }

    private HttpResponseMessage deleteDevice(Connection conn, String id, HttpRequestMessage<Optional<String>> request) throws SQLException {
        String sql = "DELETE FROM devices WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                return request.createResponseBuilder(HttpStatus.NO_CONTENT).build(); // 삭제 성공 시 204 No Content
            } else {
                return request.createResponseBuilder(HttpStatus.NOT_FOUND).body("Device not found with ID: " + id).build();
            }
        }
    }
}