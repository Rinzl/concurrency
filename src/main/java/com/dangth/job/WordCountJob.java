package com.dangth.job;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WordCountJob implements Callable<Map<String, Long>> {
    public static AtomicInteger count = new AtomicInteger(1);
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
        log.info("Read file : #{}", count.getAndIncrement());
        log.info(String.format(LOG_UUID_FORMAT, uuid.toString(), file.getAbsolutePath()));
        Map<String, Long> wordCount = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String in;
        while ((in = reader.readLine()) != null) {
            for (String s : in.toLowerCase().split(" ")) {
                wordCount.put(s, wordCount.containsKey(s) ? wordCount.get(s) + 1 : 1);
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(wordCount.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Collections.reverse(list);
        Map<String, Long> output = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            output.put(list.get(i).getKey(), list.get(i).getValue());
        }
        reader.close();
        log.info(String.format(LOG_SIZE_FORMAT, uuid.toString(), output.entrySet()));
        return output;
    }
}
