package com.dangth.job;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class WordCountJob implements Callable<Map<String, Long>> {
    public static AtomicLong totalWordCount = new AtomicLong(0);
    private static AtomicInteger fileCount = new AtomicInteger(1);
    private static final String LOG_UUID_FORMAT = "%s : reading <-- %s";
    private static final String LOG_SIZE_FORMAT = "%s : size <-- %s";
    private File file;
    private UUID uuid;

    public WordCountJob(File file) {
        this.file = file;
        this.uuid = UUID.randomUUID();
    }

    @SneakyThrows
    @Override
    public Map<String, Long> call() {
        log.info("Read file : #{}", fileCount.getAndIncrement());
        log.info(String.format(LOG_UUID_FORMAT, uuid.toString(), file.getAbsolutePath()));
        Map<String, Long> wordCount = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String in;
        while ((in = reader.readLine()) != null) {
            for (String s : in.toLowerCase().split(" ")) {
                totalWordCount.getAndIncrement();
                wordCount.put(s, wordCount.containsKey(s) ? wordCount.get(s) + 1 : 1);
            }
        }
        reader.close();
        Map<String, Long> output = wordCount
                .entrySet()
                .stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info(String.format(LOG_SIZE_FORMAT, uuid.toString(), output.entrySet()));
        return output;
    }
}
