import { NestFactory } from '@nestjs/core';
import { ClientAppModule } from './client-app.module';

async function bootstrap() {
  const app = await NestFactory.create(ClientAppModule);
  await app.listen(process.env.CLIENT_PORT ?? 3200);
}

void bootstrap();
