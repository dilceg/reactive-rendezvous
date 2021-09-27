package com.delceg.reactor.samples.errorhandling;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

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

    /**
     * note that in case of onErrorResume the flux is substituted when the first error occurs.
     * Reactor does not move onto processing further items.
     */
    @Test
    public void testThatOnErrorResumeInterruptsFlux() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("Business Exception")))
            .concatWith(Flux.just("D"))
            .onErrorResume(throwable -> {
                System.out.println("on error resume");
                return Flux.just("X", "Y", "Z");
            });

        StepVerifier.create(flux).expectNext("A")
            .expectNext("B")
            .expectNext("C")
            .expectNext("X")
            .expectNext("Y")
            .expectNext("Z")
            .verifyComplete();
//            .subscribe((type) -> System.out.println("consumer of type: " + type.toString()), (throwable) -> System.out.println("got throwable " + throwable.getClass().getName()));
    }

    @Test
    public void testInterruptedFluxTransformation() {
        var flux = Flux.just("A", "B", "C")
            .concatWith(Flux.error(new RuntimeException("foo exception")))
            .concatWith(Flux.just("D"))
            .onErrorResume(throwable -> Flux.error(new RuntimeException("wrapped", throwable)));

        var fluxLowerCase = flux.map(String::toLowerCase);

        StepVerifier.create(fluxLowerCase.log())
            .expectNext("a")
            .expectNext("b")
            .expectNext("c")
            .expectError()
            .verify();
    }

    @Test
    public void testThatOnSuccessIsInvokedOnSuccess() {
        final AtomicInteger callCount = new AtomicInteger(0);
        final Mono<String> mono = Mono.just("1").doOnSuccess(s ->
            callCount.incrementAndGet()
        );

        StepVerifier.create(mono).expectNext("1").verifyComplete();

        Assertions.assertEquals(1, callCount.get());
    }

    @Test
    public void testThatEmptyMonoDoesTriggersDoOnSuccess() {
        final AtomicInteger callCount = new AtomicInteger(0);
        final Mono<String> mono = Mono.<String>empty().doOnSuccess(s ->
            callCount.incrementAndGet()
        );

        StepVerifier.create(mono).expectNextCount(0).verifyComplete();

        Assertions.assertEquals(1, callCount.get());
    }

    /** a mono that unexpectedly throws an exception is still caught by Mono framework. */
    @Test
    public void testThatThrownExceptionGetsTransferredIntoMonoError() throws Exception {
        final Mono<String> mono = getThrowingMono();
        StepVerifier.create(mono).expectError().verify();
    }

    private Mono<String> getThrowingMono() {
        return Mono.just("a").flatMap(s -> {
            throw new IllegalArgumentException("terrible burn");
        });
    }

    @Test
    public void testOnErrorResumeWithPermissibleException() {
        // given: - an error processing scheme where some of the exceptions are recoverable:
        Function<Mono<String>, Mono<String>> errorProcessingFunction = (errorIn) ->
            errorIn.onErrorResume(
                throwable -> {
                    if (throwable.getClass().equals(IllegalArgumentException.class)) {
                        // LOG error/warning message here (if this was real code)
                        return Mono.empty();
                    } else {
                        return Mono.error(throwable);
                    }
                }
            );

        // when: - recoverable error is received
        final Mono<String> monoWithRecoverableError = errorProcessingFunction.apply(Mono.error(new IllegalArgumentException("recoverable error")));
        // then: - we should see no error thrown
        StepVerifier.create(monoWithRecoverableError)
            .expectNextCount(0).verifyComplete();

        // when: - un-recoverable error is received
        final Mono<String> monoWithUnrecoverableError = errorProcessingFunction.apply(Mono.error(new RuntimeException("terrible burn")));
        // then: - we should see the error being thrown
        StepVerifier.create(monoWithUnrecoverableError)
            .expectErrorSatisfies(throwable -> Assertions.assertEquals("terrible burn", throwable.getMessage()))
            .verify();
    }
}
