package com.delceg.reactor.samples.errorhandling;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

    @Test
    public void testFilterWhen() {
        final Mono<String> mono = Mono.just("1");
        Mono<String> filteredMono = mono.filterWhen(m -> (m.equals("1")) ? Mono.just(true) : Mono.just(false));

        StepVerifier.create(filteredMono)
            .expectNext("1").verifyComplete();
    }

    @Test
    public void testNegativeFilterWhen() {
        final Mono<String> mono = Mono.just("2");
        Mono<String> filteredMono = mono.filterWhen(m -> (m.equals("1")) ? Mono.just(true) : Mono.just(false));

        StepVerifier.create(filteredMono)
            .expectNextCount(0).verifyComplete();
    }

    // CAVEAT:
    // notice that map() operation is skipped in this case, which may mean that you are not getting some of the
    // effects you would hope for.
    @Test
    public void testMonoVoidUse_map_not_executed() {
        // given:
        final AtomicInteger counter = new AtomicInteger(0);
        final Runnable runnable = () -> counter.incrementAndGet();
        final Mono<Integer> voidMono = Mono.fromRunnable(runnable)
            .map(
                void_value -> counter.incrementAndGet() // note that this statement won't be exercised!
            );

        // expect:
        StepVerifier.create(voidMono)
            .expectNext()
            .verifyComplete();

        // and: -- note that map() operation was skipped because of Mono.fromRunnable() returning Mono<Void>.
        Assertions.assertEquals(1, counter.get());
    }

    @Test
    public void testMonoVoidUse_map_executed_successfully() {
        // given:
        final AtomicInteger counter = new AtomicInteger(0);
        final Runnable runnable = () -> counter.incrementAndGet();
        final Mono<Integer> voidMono = Mono.fromRunnable(runnable)
            .thenReturn(true) // this makes the map() statement below to execute!
            .map(
                void_value -> counter.incrementAndGet()
            );

        // expect:
        StepVerifier.create(voidMono)
            .expectNext()
            .expectNext(2)
            .verifyComplete();

        // and: -- note that map() operation was executed (compare to previous test, `testMonoVoidUse_map_not_executed()`)
        Assertions.assertEquals(2, counter.get());
    }

    /**
     * the next two tests highlight that we have evaluation of function passed in for switchIfEmpty, even if that
     * condition is not triggered.
     *
     * In the following test we ensure that the Mono is now empty, but switchIfEmpty is still executed.
     *
     * Details explained here: https://stackoverflow.com/questions/54373920/mono-switchifempty-is-always-called
     */
    @Test
    void testBehaviorOfSwitchIfEmptyEval() {
        // given:
        AtomicInteger counter = new AtomicInteger(0);
        // when:
        Mono<String> testMono = Mono.just("1")
            .switchIfEmpty(doIfEmpty(counter));

        // then:
        StepVerifier.create(testMono).expectNextCount(1).verifyComplete();
        // and: despite of not having an empty item, it is still invoked. See next test how to resolve this.
        Assertions.assertEquals(1, counter.get());
    }

    private Mono<String> doIfEmpty(AtomicInteger integer) {
        integer.incrementAndGet();
        return Mono.just("x");
    }

    /**
     * See {@link #testBehaviorOfSwitchIfEmptyEval} for details.
     *
     * In this test thanks to using Mono.defer in switchIfEmpty that clause is never executed (as expected) for empty
     * mono.
     */
    @Test
    void testBehaviorOfSwitchIfEmptyEval_lazy_exec() {
        // given:
        AtomicInteger counter = new AtomicInteger(0);
        // when:
        Mono<String> testMono = Mono.just("1")
            .switchIfEmpty(Mono.defer(() -> doIfEmpty(counter)));

        // then:
        StepVerifier.create(testMono).expectNextCount(1).verifyComplete();
        // and: thanks to lazy initialization this code is not invoked
        Assertions.assertEquals(0, counter.get());
    }

    /**
     * This demonstrates a suprising side effect of chaining multiple fluxes with flatMap().
     *
     * Even though only the first flux will fail, we will see .onErrorMap() invoked for
     * each flux in the chain.
     */
    @Test
    public void testThatMappingGetsExecutedOnError() {
        // given: - first flux that will fail
        Flux<String> firstFailingFlux = Flux.error(new IllegalArgumentException("terrible burn"));

        // and: - indicator latches of how far the code execution gets:
        AtomicBoolean visitedFirstFlux_OnErrorMap = new AtomicBoolean(false);
        AtomicBoolean visitedSecondFlux_OnErrorMap = new AtomicBoolean(false);
        AtomicBoolean visitedSecondFlux = new AtomicBoolean(false);

        // and: - first flux
        final Flux<String> firstFluxResult = firstFailingFlux
            .onErrorMap(throwable -> {
                visitedFirstFlux_OnErrorMap.set(true);
                return new RuntimeException("wrapped", throwable);
            });

        // and: - second flux chained after first flux
        final Flux<String> secondFlux = firstFluxResult.flatMap(
            personList -> {
                visitedSecondFlux.set(true);
                return Flux.just("foobar");
            }).onErrorMap(throwable -> {
            visitedSecondFlux_OnErrorMap.set(true);
            return new RuntimeException("doubleWrapped", throwable);
        });

        // expect:
        StepVerifier.create(secondFlux).expectError().verify();
        // and: - counterintuitively *both* onErrorMap() gets invoked in 1st and 2nd flux.
        Assertions.assertTrue(visitedFirstFlux_OnErrorMap.get());
        Assertions.assertTrue(visitedSecondFlux_OnErrorMap.get());
        // and: - it is expected that second flux won't get executed because the first flux failed.
        Assertions.assertEquals(false, visitedSecondFlux.get());
    }

    /**
     * this test demonstrates how to only invoke one of the doOnError()-s
     * when chaining 2 different fluxes (which can run independently) with
     * Mono.zip.
     *
     * Details about use of Mono.zip for parallel run of tasks: https://stackoverflow.com/a/48183459
     */
    @Test
    public void testThatOnlyTheFirstDoOnErrorGetsTriggered() {
        // given: - set of boolean latches to verify which part of code executed
        AtomicBoolean visited_second_OnNext = new AtomicBoolean(false);
        AtomicBoolean visited_second_OnError = new AtomicBoolean(false);
        AtomicBoolean visited_first_OnError = new AtomicBoolean(false);

        // and: first stream
        Mono<String> errorSupplier = Mono.error(new IllegalArgumentException("terrible burn"));

        // and: second stream
        Mono<String> normal = Mono.just("foo")
            .doOnNext(it -> visited_second_OnNext.set(true))
            .doOnError(it -> visited_second_OnError.set(true));

        // and: combination of the two streams with zip()
        Mono<Tuple2<String, String>> result = Mono.zip(
            errorSupplier.doOnError(e -> visited_first_OnError.set(true)),
            normal
        );

        // expect:
        StepVerifier.create(result).expectError().verify();
        // and: Note that neither of latches in the 2nd flux was triggered:
        Assertions.assertFalse(visited_second_OnNext.get());
        Assertions.assertFalse(visited_second_OnError.get());
        // and: as expected the error latch was triggered on the first flux:
        Assertions.assertTrue(visited_first_OnError.get());
    }

    /**
     * this test demonstrates how to only invoke one of the doOnError()-s
     * when chaining 2 different fluxes (which can run independently) with
     * then().
     */
    @Test
    public void testThatOnlyTheFirstDoOnErrorGetsTriggeredWhenUsingThen() {
        // given: - set of boolean latches to verify which part of code executed
        AtomicBoolean visited_second_OnNext = new AtomicBoolean(false);
        AtomicBoolean visited_second_OnError = new AtomicBoolean(false);
        AtomicBoolean visited_first_OnError = new AtomicBoolean(false);

        // and: first stream
        Mono<String> errorSupplier = Mono.error(new IllegalArgumentException("terrible burn"));

        // and: second stream
        Mono<String> normal = Mono.just("foo")
            .doOnNext(it -> visited_second_OnNext.set(true))
            .doOnError(it -> visited_second_OnError.set(true));

        // and:
        Mono<String> result = errorSupplier
            .doOnError(e -> visited_first_OnError.set(true))
            .then(normal);

        // expect:
        StepVerifier.create(result).expectError().verify();
        // and: Note that neither of latches in the 2nd flux was triggered:
        Assertions.assertFalse(visited_second_OnNext.get());
        Assertions.assertFalse(visited_second_OnError.get());
        // and: as expected the error latch was triggered on the first flux:
        Assertions.assertTrue(visited_first_OnError.get());
    }

    /**
     * this test demonstrates what it takes to correctly execute
     * .map() call after a Mono<Void> executed successfully.
     *
     * If no .thenReturn(true) is used, the .map() function has no item
     * to map, you cannot map "void".
     */
    @Test
    public void testMapIsExecutedAfterMonoVoid() {
        // given: - fake Void Mono that simulates success of method returning Mono<Void>
        Mono<Void> voidFake = Mono.empty();
        // and: - adding a thenReturn to make the mapping call happen
        Mono<Boolean> monoFake = voidFake.thenReturn(true);
        // and: - counter to verify that map method was invoked
        AtomicInteger i = new AtomicInteger(0);

        // and: - the mapping Mono call, that we asserting to execute
        Mono<Boolean> result = monoFake.map(foo -> {
            i.incrementAndGet();
            return foo;
        });

        // expect: - one item is processed
        StepVerifier.create(result).expectNextCount(1).verifyComplete();
        // and: we visited the .map() method
        Assertions.assertEquals(1, i.get());
    }
}
