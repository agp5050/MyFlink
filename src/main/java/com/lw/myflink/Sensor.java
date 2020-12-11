package com.lw.myflink;

public class Sensor {
    private String id;
    private Double temperature;
    private Long timestamp;
    public Sensor(){}
    public Sensor(String id, Double temperature, Long timestamp) {
        this.id = id;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id='" + id + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
