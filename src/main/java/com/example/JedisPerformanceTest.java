package com.example;

import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class JedisPerformanceTest {
    private final JedisPool jedisPool;

    public JedisPerformanceTest(String redisHost, int redisPort) {
        this.jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    }

    public static void main(String[] args) {
        String redisHost = "localhost";
        int redisPort = 6379;

        JedisPerformanceTest test = new JedisPerformanceTest(redisHost, redisPort);

        try {
            // Run a performance test
            test.performanceTest();

        } catch (Exception e) {
            System.err.println("Error during operation: " + e.getMessage());
            e.printStackTrace();
        } finally {
            test.close();
        }
    }

    public void performanceTest() throws ExecutionException, InterruptedException {
        int operationsPerBatch = 500;
        int numberOfBatches = 50000;
        int keySize = 16;
        int totalOperations = numberOfBatches * operationsPerBatch;


        // Generate keys of the specified size
        List<String> keys = new ArrayList<>(operationsPerBatch);
        String baseKey = "A";
        for (int i = 0; i < operationsPerBatch; i++) {
            String key = generateKey(baseKey, keySize, i);
            keys.add(key);
        }
        String baseField = "field:";
        List<String> fields = new ArrayList<>(operationsPerBatch);
        List<String> values = new ArrayList<>(operationsPerBatch);
        for (int i = 0; i < operationsPerBatch; i++) {
            fields.add(baseField + i);
            values.add("testValue:" + i);
        }

        List<Double> hsetTimes = new ArrayList<>();
        List<Double> hgetTimes = new ArrayList<>();
        
        // Run a fixed number of batches
        try (Jedis jedis = jedisPool.getResource()) {
            for (int batch = 0; batch < numberOfBatches; batch++) {
                // HSET performance
                long start = System.nanoTime();
                Pipeline pipeline = jedis.pipelined();
                for (int i = 0; i < operationsPerBatch; i++) {
                    pipeline.hset(keys.get(i), fields.get(i), values.get(i));
                }
                pipeline.sync();
                long end = System.nanoTime();
                double elapsedSec = (end - start) / 1_000_000_000.0;
                double opsPerSec = operationsPerBatch / elapsedSec;
                hsetTimes.add(opsPerSec);
                System.out.println("Batch " + (batch + 1) + " HSET Performance: " 
                + operationsPerBatch + " ops in " + elapsedSec + "s => " + opsPerSec + " ops/s");

                // HGET performance
                start = System.nanoTime();
                for (int i = 0; i < operationsPerBatch; i++) {
                    pipeline.hget(keys.get(i), fields.get(i));
                }
                pipeline.sync();
                end = System.nanoTime();
                elapsedSec = (end - start) / 1_000_000_000.0;
                opsPerSec = operationsPerBatch / elapsedSec;
                hgetTimes.add(opsPerSec);
                System.out.println("Batch " + (batch + 1) + " HGET Performance: " 
                + operationsPerBatch + " ops in " + elapsedSec + "s => " + opsPerSec + " ops/s");
            }

            // Mean calculations
            double hsetMean = calculateMean(hsetTimes);
            double hgetMean = calculateMean(hgetTimes);

            System.out.println("Mean HSET Ops/s over " + numberOfBatches 
            + " batches with each batch having " + operationsPerBatch 
            + " operations, totalling " + totalOperations 
            + " is " + hsetMean);
            System.out.println("Mean HGET Ops/s over " + numberOfBatches 
            + " batches with each batch having " + operationsPerBatch 
            + " operations, totalling " + totalOperations 
            + " is " + hgetMean);

            System.out.println("Performance test was completed with key size: " + keySize + " bytes");
        }
    }

    private static double calculateMean(List<Double> values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.size();
    }

    private static String generateKey(String baseKey, int size, int index) {
        StringBuilder keyBuilder = new StringBuilder(baseKey);
        while (keyBuilder.length() < size - Integer.toString(index).length()) {
            keyBuilder.append("A"); // Pad with 'A'
        }
        keyBuilder.append(index); // Append index to ensure unique keys
        return keyBuilder.toString();
    }
    
    public void close() {
        jedisPool.close();
    }
}
