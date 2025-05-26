Let's consider a real-world scenario for using the Circuit Breaker pattern in a NestJS microservices architecture, drawing from the explanation in the circuit_breaker_pattern_explanation_2025 document.

# Scenario: E-commerce Order Processing

Imagine you have a microservices-based e-commerce platform built with NestJS:

- Order Service: Handles incoming orders, orchestrates the order fulfillment process.
- Inventory Service: Manages stock levels for products.
- Payment Service: Processes payments with an external payment gateway.
- Notification Service: Sends email/SMS notifications to customers.

## The Problem Point: Calling the Payment Service

When a customer places an order, the Order Service needs to call the Payment Service to authorize and capture the payment. The Payment Service, in turn, communicates with an external, third-party payment gateway (e.g., Stripe, PayPal).

This external payment gateway is a critical dependency but also a potential point of failure or slowdown:

- It might be temporarily down for maintenance.
- It might be experiencing high load and responding slowly.
- Network issues could exist between your Payment Service and the gateway.
- The gateway might return transient errors (e.g., "try again later").

## Without a Circuit Breaker:

If the external payment gateway is slow or failing, and the Order Service (through the Payment Service) keeps trying to process payments:

1. Resource Hogging in Order Service & Payment Service: HTTP request connections from Order Service to Payment Service, and from Payment Service to the gateway, will pile up. Threads/async handlers will be busy waiting for timeouts. This can exhaust resources in both services.
2. Poor User Experience: Customers trying to place orders will experience very long loading times or their requests will eventually time out, leading to frustration and potentially abandoned carts.
3. Cascading Failure: If the Order Service becomes unresponsive because all its resources are tied up waiting for the Payment Service, new incoming orders might fail even before reaching the payment step. Other services calling the Order Service might also start to fail.
4. Overwhelming the Gateway: If the gateway is already struggling, continuous retries from your Payment Service can exacerbate its problems.

## Introducing the Circuit Breaker in the Payment Service (or Order Service when calling Payment Service)

Let's implement a Circuit Breaker within the Payment Service for calls it makes to the external payment gateway. (Alternatively, the Order Service could have a circuit breaker for its calls to the Payment Service if the Payment Service itself is the bottleneck, but for this example, we'll focus on the external gateway).

NestJS Implementation Sketch (Conceptual - using a hypothetical library like opossum):

Let's assume you're using a library like opossum for circuit breaking in your PaymentService.

```tsc
// payment.service.ts (Conceptual)
import { Injectable, HttpException, HttpStatus } from '@nestjs/common';
import CircuitBreaker from 'opossum'; // Example library

interface PaymentDetails {
  amount: number;
  currency: string;
  cardToken: string;
  // ... other details
}

@Injectable()
export class PaymentService {
  private paymentGatewayClient: any; // Your actual client to call the external gateway
  private circuitBreaker: CircuitBreaker;

  constructor() {
    // Initialize your actual payment gateway client
    this.paymentGatewayClient = {
      processTransaction: async (details: PaymentDetails): Promise<{ success: boolean; transactionId?: string; error?: string }> => {
        // Simulate external API call
        console.log(`[PaymentGatewayClient] Attempting to process payment for ${details.amount}`);
        // Simulate potential failures or delays
        const random = Math.random();
        if (random < 0.3) { // 30% chance of failure
          console.error('[PaymentGatewayClient] Simulated Gateway Failure');
          throw new Error('Gateway Timeout or Internal Error');
        }
        if (random < 0.6) { // 30% chance of slow response
          console.log('[PaymentGatewayClient] Simulated Gateway Slow Response...');
          await new Promise(resolve => setTimeout(resolve, 6000)); // 6-second delay
        }
        console.log('[PaymentGatewayClient] Simulated Gateway Success');
        return { success: true, transactionId: `txn_${Date.now()}` };
      },
    };

    const options = {
      timeout: 3000, // If the function doesn't Fulfill/Reject in 3 seconds, trigger a timeout
      errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
      resetTimeout: 15000, // After 15 seconds in OPEN state, attempt to send a request (HALF-OPEN)
    };
    this.circuitBreaker = new CircuitBreaker(
      (details: PaymentDetails) => this.paymentGatewayClient.processTransaction(details),
      options,
    );

    // Optional: Define fallback behavior
    this.circuitBreaker.fallback(() => {
      console.warn('[CircuitBreaker] Fallback: Payment processing temporarily unavailable.');
      // Could return a specific error, a "try later" message, or queue the payment
      return {
        success: false,
        error: 'Payment service temporarily unavailable. Please try again later.',
        fallback: true,
      };
    });

    // Optional: Listen to events for logging/monitoring
    this.circuitBreaker.on('open', () => console.log(`[CircuitBreaker] State: OPEN - Circuit tripped at ${new Date().toISOString()}`));
    this.circuitBreaker.on('halfOpen', () => console.log(`[CircuitBreaker] State: HALF-OPEN - Attempting test request at ${new Date().toISOString()}`));
    this.circuitBreaker.on('close', () => console.log(`[CircuitBreaker] State: CLOSED - Circuit reset at ${new Date().toISOString()}`));
    this.circuitBreaker.on('failure', (error, executionTime) => console.error(`[CircuitBreaker] Call FAILED in ${executionTime}ms: ${error.message}`));
    this.circuitBreaker.on('success', (result, executionTime) => console.log(`[CircuitBreaker] Call SUCCEEDED in ${executionTime}ms`));
    this.circuitBreaker.on('timeout', (executionTime) => console.warn(`[CircuitBreaker] Call TIMED OUT after ${executionTime}ms`));
    this.circuitBreaker.on('reject', (executionTime) => console.warn(`[CircuitBreaker] Call REJECTED (circuit open) after ${executionTime}ms`)); // When OPEN
    this.circuitBreaker.on('fallback', (data, error) => console.warn(`[CircuitBreaker] Fallback executed due to: ${error?.message || 'Circuit Open'}`));
  }

  async processPayment(paymentDetails: PaymentDetails): Promise<any> {
    console.log(`[PaymentService] Received payment processing request for ${paymentDetails.amount}`);
    try {
      // `fire` will execute the function passed to the CircuitBreaker constructor
      const result = await this.circuitBreaker.fire(paymentDetails);

      if (result.fallback && !result.success) {
        // Handle the fallback response appropriately
        throw new HttpException(result.error, HttpStatus.SERVICE_UNAVAILABLE);
      }
      if (!result.success) {
        throw new HttpException(result.error || 'Payment processing failed', HttpStatus.BAD_REQUEST);
      }
      return result;
    } catch (error) {
      // This catch block will handle errors from circuitBreaker.fire()
      // including when the circuit is open (rejects immediately) or when the underlying call fails.
      // The fallback might have already transformed the error.
      if (error instanceof HttpException) {
        throw error;
      }
      // If it's not an HttpException, it might be an unexpected error or one from the fallback itself
      console.error('[PaymentService] Unexpected error during payment processing:', error);
      throw new HttpException(
        error.message || 'An unexpected error occurred during payment processing.',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
```

## How it Behaves (referring to circuit_breaker_pattern_explanation_2025):

1. CLOSED State (Normal Operation):

- The Order Service calls PaymentService.processPayment().
- this.circuitBreaker.fire(paymentDetails) allows the call to this.paymentGatewayClient.processTransaction().
- If the external gateway responds successfully within the timeout (3 seconds), the circuit remains CLOSED.
- If calls start failing or timing out:
  - The circuit breaker counts these failures.
  - Once the errorThresholdPercentage (50%) is met, the circuit breaker trips and moves to the OPEN state. The open event is logged.

2. OPEN State (Gateway is Unhealthy):

- Now, when Order Service calls PaymentService.processPayment(), this.circuitBreaker.fire() immediately rejects the call without contacting the external gateway.
- The reject event is logged.
- The defined fallback function is executed. In our example, it returns { success: false, error: 'Payment service temporarily unavailable...', fallback: true }.
- The PaymentService.processPayment() method throws an HttpException with status SERVICE_UNAVAILABLE.
- The Order Service receives this immediate error and can handle it gracefully (e.g., inform the user to try again later, save the order in a "pending payment" state).
- This state lasts for resetTimeout (15 seconds). During this time, the external payment gateway is shielded from requests, giving it a chance to recover.

3. HALF-OPEN State (Testing for Recovery):

- After 15 seconds in the OPEN state, the circuit breaker transitions to HALF-OPEN. The halfOpen event is logged.
- The next call to PaymentService.processPayment() will be allowed through by this.circuitBreaker.fire() as a trial request to the external gateway.
- If this trial request succeeds: The circuit breaker assumes the gateway is healthy again, transitions to CLOSED (logging the close event), and resets its failure counters. Normal operation resumes.
- If this trial request fails (or times out): The circuit breaker assumes the gateway is still unhealthy, transitions back to OPEN (logging the open event), and restarts the resetTimeout (another 15 seconds).

## Benefits in this NestJS Microservice Context:

- Order Service Resilience: The Order Service doesn't get bogged down waiting for a failing Payment Service (which is failing due to the external gateway). It gets a quick response (error or fallback).
- Payment Service Stability: The Payment Service itself doesn't exhaust its resources trying to call a dead or slow external gateway.
- Gateway Recovery: The external payment gateway gets a break from traffic, increasing its chances of recovery.
- Improved User Experience: Customers get faster feedback if payment processing is down, rather than an infinitely spinning wheel.
- Monitoring: By logging the circuit breaker state changes (open, halfOpen, close), you gain valuable insights into the health and reliability of the external payment gateway. This can be fed into your monitoring and alerting systems.

This scenario demonstrates how the Circuit Breaker pattern, as described in circuit_breaker_pattern_explanation_2025, can be practically applied in a NestJS microservice to protect your system from the unreliability of its dependencies.

# How to run and test the source code

```
pnpm install
pnpm start:dev
```

```
curl -X POST http://localhost:3000/payment/process \
-H "Content-Type: application/json" \
-d '{
  "amount": 100,
  "currency": "USD",
  "cardToken": "your-card-token"
}'
```
