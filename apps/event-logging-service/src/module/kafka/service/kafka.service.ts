// import { Injectable } from "@nestjs/common";
// import { ConfigService } from "@nestjs/config";
// import { Kafka } from "kafkajs";
// import * as console from 'node:console';
//
// @Injectable()
// export class KafkaService {
//   private readonly kafkaClient: Kafka;
//
//   constructor(
//     private readonly configService: ConfigService
//   ) {
//     console.log((this.configService.get("KAFKA_URL") as any).replaceAll('kafka+ssl://', ''))
//     this.kafkaClient = new Kafka({
//       brokers: (this.configService.get("KAFKA_URL") as any).replaceAll('kafka+ssl://', ''),
//       ssl: true,
//       sasl: {
//         mechanism: "scram-sha-256",
//         username: this.configService.get<string>("CLOUDKARAFKA_USERNAME"),
//         password: this.configService.get<string>("CLOUDKARAFKA_PASSWORD")
//       },
//       clientId: "kafka-service",
//       connectionTimeout: 300000,
//       requestTimeout: 25000
//     });
//   }
//
//   public getKafkaClient(): Kafka {
//     return this.kafkaClient;
//   }
// }


import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { Kafka, KafkaConfig } from "kafkajs";
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class KafkaService {
  private readonly kafkaClient: Kafka;

  constructor(private readonly configService: ConfigService) {
    // Fetch and process environment variables
    const brokers = this.configService.get<string>("KAFKA_URL")?.replace('kafka+ssl://', '');
    // const username = this.configService.get<string>("CLOUDKARAFKA_USERNAME");
    // const password = this.configService.get<string>("CLOUDKARAFKA_PASSWORD");
    const trustedCert = this.configService.get<string>("KAFKA_TRUSTED_CERT");
    const clientCertKey = this.configService.get<string>("KAFKA_CLIENT_CERT_KEY");
    const clientCert = this.configService.get<string>("KAFKA_CLIENT_CERT");

    if (!brokers || !trustedCert || !clientCertKey || !clientCert) {
      throw new Error("Kafka configuration is missing required environment variables.");
    }

    // Write SSL certificates to temporary files
    const caPath = path.join(__dirname, 'ca_cert.pem');
    const keyPath = path.join(__dirname, 'client_cert_key.pem');
    const certPath = path.join(__dirname, 'client_cert.pem');

    fs.writeFileSync(caPath, trustedCert);
    fs.writeFileSync(keyPath, clientCertKey);
    fs.writeFileSync(certPath, clientCert);

    // Kafka configuration
    const kafkaConfig: KafkaConfig = {
      brokers: brokers.split(","),
      ssl: {
        rejectUnauthorized: false,
        ca: [fs.readFileSync(caPath, 'utf-8')],
        key: fs.readFileSync(keyPath, 'utf-8'),
        cert: fs.readFileSync(certPath, 'utf-8'),
      },
      clientId: "kafka-service",
      connectionTimeout: 300000,
      requestTimeout: 25000,
    };

    this.kafkaClient = new Kafka(kafkaConfig);

    // Clean up temporary files on exit
    process.on('exit', () => {
      fs.unlinkSync(caPath);
      fs.unlinkSync(keyPath);
      fs.unlinkSync(certPath);
    });
  }

  public getKafkaClient(): Kafka {
    return this.kafkaClient;
  }
}