/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
// payment.service.ts (Conceptual)
import { Injectable, HttpException, HttpStatus } from '@nestjs/common';
import * as CircuitBreaker from 'opossum';

interface PaymentDetails {
  amount: number;
  currency: string;
  cardToken: string;
  // ... other details
}

@Injectable()
export class PaymentService {
  private paymentGatewayClient: any; // Your actual client to call the external gateway
  private circuitBreaker: CircuitBreaker<PaymentDetails>;

  constructor() {
    // Initialize your actual payment gateway client
    this.paymentGatewayClient = {
      processTransaction: async (
        details: PaymentDetails,
      ): Promise<{
        success: boolean;
        transactionId?: string;
        error?: string;
      }> => {
        // Simulate external API call
        console.log(
          `[PaymentGatewayClient] Attempting to process payment for ${details.amount}`,
        );
        // Simulate potential failures or delays
        const random = Math.random();
        if (random < 0.3) {
          // 30% chance of failure
          console.error('[PaymentGatewayClient] Simulated Gateway Failure');
          throw new Error('Gateway Timeout or Internal Error');
        }
        if (random < 0.6) {
          // 30% chance of slow response
          console.log(
            '[PaymentGatewayClient] Simulated Gateway Slow Response...',
          );
          await new Promise((resolve) => setTimeout(resolve, 6000)); // 6-second delay
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
      (details: PaymentDetails) =>
        this.paymentGatewayClient.processTransaction(details),
      options,
    );

    // Optional: Define fallback behavior
    this.circuitBreaker.fallback(() => {
      console.warn(
        '[CircuitBreaker] Fallback: Payment processing temporarily unavailable.',
      );
      // Could return a specific error, a "try later" message, or queue the payment
      return {
        success: false,
        error:
          'Payment service temporarily unavailable. Please try again later.',
        fallback: true,
      };
    });

    // Optional: Listen to events for logging/monitoring
    this.circuitBreaker.on('open', () =>
      console.log(
        `[CircuitBreaker] State: OPEN - Circuit tripped at ${new Date().toISOString()}`,
      ),
    );
    this.circuitBreaker.on('halfOpen', () =>
      console.log(
        `[CircuitBreaker] State: HALF-OPEN - Attempting test request at ${new Date().toISOString()}`,
      ),
    );
    this.circuitBreaker.on('close', () =>
      console.log(
        `[CircuitBreaker] State: CLOSED - Circuit reset at ${new Date().toISOString()}`,
      ),
    );
    this.circuitBreaker.on('failure', (error, executionTime) =>
      console.error(
        `[CircuitBreaker] Call FAILED in ${executionTime}ms: ${error.message}`,
      ),
    );
    this.circuitBreaker.on('success', (result, executionTime) =>
      console.log(`[CircuitBreaker] Call SUCCEEDED in ${executionTime}ms`),
    );
    this.circuitBreaker.on('timeout', (executionTime) =>
      console.warn(`[CircuitBreaker] Call TIMED OUT after ${executionTime}ms`),
    );
    this.circuitBreaker.on('reject', (executionTime) =>
      console.warn(
        `[CircuitBreaker] Call REJECTED (circuit open) after ${executionTime}ms`,
      ),
    ); // When OPEN
    this.circuitBreaker.on('fallback', (data, error) =>
      console.warn(
        `[CircuitBreaker] Fallback executed due to: ${error?.message || 'Circuit Open'}`,
      ),
    );
  }

  async processPayment(paymentDetails: PaymentDetails): Promise<any> {
    console.log(
      `[PaymentService] Received payment processing request for ${paymentDetails.amount}`,
    );
    try {
      // `fire` will execute the function passed to the CircuitBreaker constructor
      const result = await this.circuitBreaker.fire(paymentDetails);

      if (result.fallback && !result.success) {
        // Handle the fallback response appropriately
        throw new HttpException(result.error, HttpStatus.SERVICE_UNAVAILABLE);
      }
      if (!result.success) {
        throw new HttpException(
          result.error || 'Payment processing failed',
          HttpStatus.BAD_REQUEST,
        );
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
      console.error(
        '[PaymentService] Unexpected error during payment processing:',
        error,
      );
      throw new HttpException(
        error.message ||
          'An unexpected error occurred during payment processing.',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
