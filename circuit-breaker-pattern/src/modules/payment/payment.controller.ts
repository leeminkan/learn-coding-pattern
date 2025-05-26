/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  Controller,
  Post,
  Body,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { PaymentService } from './payment.service';

interface PaymentRequestDto {
  amount: number;
  currency: string;
  cardToken: string;
  // ... other details
}

@Controller('payment')
export class PaymentController {
  constructor(private readonly paymentService: PaymentService) {}

  @Post('process')
  async processPayment(@Body() paymentRequest: PaymentRequestDto) {
    try {
      console.log(
        `[PaymentController] Processing payment for ${paymentRequest.amount}`,
      );
      const result = await this.paymentService.processPayment(paymentRequest);
      return {
        success: true,
        message: 'Payment processed successfully',
        data: result,
      };
    } catch (error) {
      console.error('[PaymentController] Error processing payment:', error);

      if (error instanceof HttpException) {
        throw error;
      }

      throw new HttpException(
        'An unexpected error occurred while processing the payment.',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
