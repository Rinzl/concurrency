package com.dangth.util;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DataReader {

    public static List<File> getTextFileList(String path) {
        File[] files = Objects.requireNonNull(new File(path).listFiles());
        return Arrays
                .stream(files)
                .filter(f -> f.getAbsolutePath().endsWith(".txt"))
                .collect(Collectors.toList());
    }
}
