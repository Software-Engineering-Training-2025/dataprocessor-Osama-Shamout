package org.example.dataprocessor;

import org.example.dataprocessor.enums.AnalysisType;
import org.example.dataprocessor.enums.CleaningType;
import org.example.dataprocessor.enums.OutputType;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class DataProcessorService {

    public double process(
        CleaningType cleaningType,
        AnalysisType analysisType,
        OutputType outputType,
        List<Integer> data) throws Exception {

        List<Integer> cleaned = new ArrayList<>(data);

        if (cleaningType == CleaningType.REMOVE_NEGATIVES) {
            cleaned = cleaned.stream()
                    .filter(n -> n >= 0)
                    .collect(Collectors.toList());
        } else if (cleaningType == CleaningType.REPLACE_NEGATIVES_WITH_ZERO) {
            cleaned = cleaned.stream()
                    .map(n -> n < 0 ? 0 : n)
                    .collect(Collectors.toList());
        }

        double result;
        switch (analysisType) {
            case MEAN:
                result = cleaned.isEmpty()
                        ? Double.NaN
                        : cleaned.stream().mapToDouble(i -> i).average().orElse(Double.NaN);
                break;

            case MEDIAN:
                if (cleaned.isEmpty()) {
                    result = Double.NaN;
                } else {
                    List<Integer> sorted = new ArrayList<>(cleaned);
                    Collections.sort(sorted);
                    int n = sorted.size();
                    if (n % 2 == 1) {
                        result = sorted.get(n / 2);
                    } else {
                        result = (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
                    }
                }
                break;

            case STD_DEV:
                if (cleaned.isEmpty()) {
                    result = Double.NaN;
                } else {
                    double mean = cleaned.stream().mapToDouble(i -> i).average().orElse(0.0);
                    double variance = cleaned.stream()
                            .mapToDouble(i -> Math.pow(i - mean, 2))
                            .sum() / cleaned.size();
                    result = Math.sqrt(variance);
                }
                break;

            case P90_NEAREST_RANK:
                if (cleaned.isEmpty()) {
                    result = Double.NaN;
                } else {
                    List<Integer> sorted = new ArrayList<>(cleaned);
                    Collections.sort(sorted);
                    int n = sorted.size();
                    int rank = (int) Math.ceil(0.90 * n); // 1-based index
                    result = sorted.get(rank - 1);
                }
                break;

            case TOP3_FREQUENT_COUNT_SUM:
                if (cleaned.isEmpty()) {
                    result = 0.0;
                } else {
                    Map<Integer, Long> freqMap = cleaned.stream()
                            .collect(Collectors.groupingBy(i -> i, Collectors.counting()));

                    result = freqMap.entrySet().stream()
                            .sorted((a, b) -> {
                                int cmp = Long.compare(b.getValue(), a.getValue());
                                if (cmp == 0) return Integer.compare(a.getKey(), b.getKey());
                                return cmp;
                            })
                            .limit(3)
                            .mapToLong(Map.Entry::getValue)
                            .sum();
                }
                break;

            default:
                throw new UnsupportedOperationException("Unknown AnalysisType: " + analysisType);
        }

        String output = "Result = " + result;
        switch (outputType) {
            case CONSOLE -> System.out.println(output);
            case TEXT_FILE -> {
                Path path = Paths.get("target/result.txt");
                Files.createDirectories(path.getParent());
                Files.writeString(path, output,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            }
        }

        // Return result
        return result;
    }
}
