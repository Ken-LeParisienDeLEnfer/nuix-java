import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Top values to return
     */
    public static final int TOP_THREE = 3;

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        Map<Integer, Integer> ageCounts = new HashMap<>();

        try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
            iterator.process(ageCounts);
        } catch (Exception e) {
            throw new RuntimeException("[ERROR]: Error while processing region: " + region, e);
        }

        return computeTop3AgesFromMap(ageCounts);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES}.
     */
    public String[] top3Ages(List<String> regionNames) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<Map<Integer, Integer>>> futures = new ArrayList<>();

        for (String region : regionNames) {
            futures.add(executorService.submit(() -> {
                Map<Integer, Integer> ageCounts = new HashMap<>();
                try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
                    iterator.process(ageCounts);
                } catch (Exception e) {
                    throw new RuntimeException("[ERROR]: Error while processing region " + region, e);
                }
                return ageCounts;
            }));
        }

        Map<Integer, Integer> combinedAgeCounts = new HashMap<>();
        for (Future<Map<Integer, Integer>> future : futures) {
            try {
                Map<Integer, Integer> ageCounts = future.get();
                ageCounts.forEach((age, count) -> combinedAgeCounts.merge(age, count, Integer::sum));
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("[ERROR] Error while combining results for a region: " + e.getCause().getMessage());
            }
        }

        executorService.shutdown();
        return computeTop3AgesFromMap(combinedAgeCounts);
    }

    private String[] computeTop3AgesFromMap(Map<Integer, Integer> ageCounts) {
        List<Map.Entry<Integer, Integer>> sortedEntries = ageCounts.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().equals(e1.getValue()) ? Integer.compare(e1.getKey(), e2.getKey()) : Integer.compare(e2.getValue(), e1.getValue()))
                .collect(Collectors.toList());

        List<String> result = new ArrayList<>();
        int currentRank = 0;
        int previousCount = -1;
        for (Map.Entry<Integer, Integer> entry : sortedEntries) {
            if (entry.getValue() != previousCount) {
                currentRank++;
            }
            if (currentRank <= TOP_THREE) {
                result.add(String.format(OUTPUT_FORMAT, currentRank, entry.getKey(), entry.getValue()));
            } else {
                break;
            }
            previousCount = entry.getValue();
        }

        return result.toArray(String[]::new);
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
        default void process(Map<Integer, Integer> ageCounts) {
            while (hasNext()) {
                int age = next();
                if (age < 0) {
                    continue;
                }
                ageCounts.merge(age, 1, Integer::sum);
            }
        }
    }
}
