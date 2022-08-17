package com.delceg.reactor.samples.performance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;

public class PerformanceTests {

    public static final int SLEEP_MILLIS_AMOUNT = 10;

    @Test @DisplayName("test that flatmap() takes how much time to complete")
    public void test1() {
        // given:
        Flux<String> raw = Flux.just("A", "B", "C");
        // when: - flatMap() is used to call a synchronous function (`item.toLowerCase()`)
        Flux<String> withFlatMap = raw.flatMap(item -> {
            sleepFor(SLEEP_MILLIS_AMOUNT);
            return Flux.just(item.toLowerCase());
        });

        // then: - expect the results, and timing be around 55 milliseconds (on 2019 macbook pro). Compare this to next test!
        printElapsedTime(() ->
                StepVerifier.create(withFlatMap)
                    .expectNext("a", "b", "c")
                    .verifyComplete()
            , "flatMap"
        );
    }

    @Test @DisplayName("test that map() takes how much time to complete")
    public void test2() {
        // given:
        Flux<String> raw2 = Flux.just("A", "B", "C");
        // when: - map() is used to call a synchronous function (`item.toLowerCase()`)
        Flux<String> withMap = raw2.map(item -> {
            sleepFor(SLEEP_MILLIS_AMOUNT);
            return item.toLowerCase();
        });

        // then: - expect the results, and timing be around 36 milliseconds (on 2019 macbook pro). Compare this to test above -- ti is *lower*!
        printElapsedTime(() ->
                StepVerifier.create(withMap)
                    .expectNext("a", "b", "c")
                    .verifyComplete()
            , "map");
    }

    private void sleepFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void printElapsedTime(Runnable runnable, String label) {
        Instant start = Instant.now();
        runnable.run();
        Instant end = Instant.now();
        System.err.println(label + " - " + (end.toEpochMilli() - start.toEpochMilli()));
    }
}
