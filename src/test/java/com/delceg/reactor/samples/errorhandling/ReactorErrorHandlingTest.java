package com.delceg.reactor.samples.errorhandling;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

// inspired by https://www.youtube.com/watch?v=Lu5p0vndcYE
public class ReactorErrorHandlingTest {

    @Test
    public void testErrorHandling() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Some error")))
            .concatWith(Flux.just("D"));

        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectError()
            .verify();
    }

    @Test
    public void testSuccessful() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.just("D"));

        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C", "D")
            .verifyComplete();
    }

    @Test
    public void testDoOnError() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Some error")))
            .doOnError(error -> System.out.println("Some error occured: " + error));
        // above prints:
        //      Some error occured: java.lang.RuntimeException: Some error

        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectError()
            .verify();
    }

    @Test
    public void testThatFlowIsInterrupted() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Some error")))
            .concatWith(Flux.just("D"));

        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectError()
            .verify(); // we never got to "D" element
    }

    @Test
    public void testOnErrorReturn() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Some error")))
            .concatWith(Flux.just("D"))
            .onErrorReturn("Some Default Value");

        // note that we never got the "D" element
        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectNext("Some Default Value")
            .verifyComplete();
    }

    @Test
    public void testOnErrorResume() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Some error")))
            .concatWith(Flux.just("D"))
            .onErrorResume(throwable -> {
                System.out.println("Some Default Value: " + throwable.getMessage());
                return Flux.just("X");
            });

        // note that we never got the "D" element
        // but got "X" instead
        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectNext("X")
            .verifyComplete();
    }

    @Test
    public void testOnErrorMap() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Business Exception")))
            .concatWith(Flux.just("D"))
            .onErrorMap(throwable -> {
                if (throwable.getMessage() == "Business Exception") {
                    return new IllegalArgumentException("Translates Exception");
                } else {
                    return throwable;
                }
            });

        // note that we never got the "D" element
        StepVerifier.create(flux.log())
            .expectNext("A", "B", "C")
            .expectErrorMessage("Translates Exception")
            .verify();
    }

    /** similar to <code>try {} finally</code> */
    @Test
    public void testFinallyTest() {
        var flux = Flux.just("A", "B", "C")
            .delayElements(Duration.ofSeconds(1))
            .doFinally(signalType -> {
                // this could be used as try { ... } finally { ... }
                switch(signalType) {
                    case CANCEL:
                        System.out.println("Perform operation on cancel");
                        break;
                    case ON_ERROR:
                        System.out.println("Perform operation on error");
                        break;
                    case ON_COMPLETE:
                        System.out.println("Perform operation on complete");
                        break;
                }
            })
            .log()
            .take(2); // this will only take first 2 elements from the stream

        StepVerifier.create(flux.log())
            .expectNext("A", "B")
            .thenCancel()
            .verify();
    }
}
