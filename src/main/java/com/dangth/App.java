package com.dangth;

import com.dangth.job.WordCountJob;
import com.dangth.util.DataReader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class App {
    public static void main(String[] args) {
        List<File> fileList = DataReader.getTextFileList("C:\\Users\\tranh\\Downloads\\Gutenberg\\txt");

        final List<WordCountJob> wordCountJobList = new ArrayList<>();
        fileList.forEach(f -> wordCountJobList.add(new WordCountJob(f)));
        runWordCountMultiThreads(wordCountJobList);

    }

    @SneakyThrows
    private static void runWordCountMultiThreads(List<WordCountJob> wordCountJobs) {
        final Map<String, Long> wordCountOutput = new HashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<Future<Map<String, Long>>> futures = executorService.invokeAll(wordCountJobs);
        for (Future<Map<String, Long>> future : futures) {
            Map<String, Long> wordCount = future.get();
            wordCount.forEach((k, v) -> wordCountOutput.put(k, wordCountOutput.containsKey(k) ? wordCountOutput.get(k) + v : v));
        }
        wordCountOutput
                .entrySet()
                .stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                .limit(10)
                .collect(Collectors.toList())
                .forEach(e -> log.info("{}", e));
        executorService.awaitTermination(45, TimeUnit.SECONDS);
        executorService.shutdownNow();

    }
}
