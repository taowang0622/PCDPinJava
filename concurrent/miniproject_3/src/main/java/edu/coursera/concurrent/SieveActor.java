package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determine the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor actor = new SieveActorActor(2);
        //Must wait for all tasks to finish before we compute the number of prime numbers======>use finish{}
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                // Actor model has a builtin FIFO message queue(unbounded/bounded???I guess unbounded like ExecutorService thread pool)
                // messages sent here would be stored in the queue!!
                // ACTOR MODEL IS A MESSAGE QUEUE IN SOME WAY!!!!
                actor.send(i); //Be aware that inside the Lambda expression, the outer variable must be final that is compliant to the idea of functional programming!!
            }
        });

        int numOfPrim = 1;
        SieveActorActor loopActor = actor; //since actor is final, I create another SieveActorActor to get a copy!!!
        while (loopActor.getNextActor() != null) {
            numOfPrim++;
            loopActor = loopActor.getNextActor();
        }

        return numOfPrim;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        //local state
        private int localPrim;
        private SieveActorActor nextActor; //for constructing actor-based pipeline!!

        //Initialize the local state of the current actor
        private SieveActorActor(int localPrim) {
            this.localPrim = localPrim;
            this.nextActor = null;
        }


        /**
         * Process a single message sent to this actor.
         * <p>
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            Integer candidate = (Integer) msg;

            boolean isMultipleOfLocalPrim = ((candidate % localPrim) == 0);  // SIEVE!!!!!!!Non Multiple of local prime!!
            if (!isMultipleOfLocalPrim) {
                if (nextActor == null) { //Avoiding the null pointer exception and at first nextActor must be null!!
                    nextActor = new SieveActorActor(candidate);  // assert candidate here is a prime number!!!
                    nextActor.send(candidate);
                } else {
                    nextActor.send(candidate);
                }
            } //If it's the multiple of the local prime, then swallow it, that means not passing it to next stage/actor of the pipeline
        }

        public SieveActorActor getNextActor() {
            return nextActor;
        }
    }

    public static void main(String[] args) {
        new SieveActor().countPrimes(10);

    }
}
