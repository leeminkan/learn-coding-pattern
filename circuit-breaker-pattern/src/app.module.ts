import { Module } from '@nestjs/common';
import { PaymentModule } from './modules/payment/payment.module';

@Module({
  imports: [PaymentModule],
  controllers: [],
  providers: [],
})
export class AppModule {}
