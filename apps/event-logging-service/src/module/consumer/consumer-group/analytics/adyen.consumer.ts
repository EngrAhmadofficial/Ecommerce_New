import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers } from "@app/common";
import { LogService } from "@app/common";

@Injectable()
export class AdyenConsumer implements OnModuleInit, InitConsumers {
  private readonly logger = new Logger(AdyenConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly analyticsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly logService: LogService
  ) {
    this.noOfConsumers = this.configService.get<number>("ADYEN_CONSUMERS");
  }

  /**
   * Module init method.
   */
  async onModuleInit(): Promise<void> {
    await this.initConsumers();
  }

  /**
   * Method to start all consumers at once.
   * @private
   */
  async initConsumers() {
    let consumerList: any = [];
    for (let i = 0; i < this.noOfConsumers; i++) {
      consumerList.push(this.startAdyenConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start Adyen consumer.
   * Consumes 'Adyen' Events
   * @private
   */
  private async startAdyenConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getAdyenTopic()}` });
      await this.analyticsConsumer.consume(
        { topics: [this.getAdyenTopic()], fromBeginning: false },
        this.getAdyenGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.logService.createLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getAdyenTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getAdyenTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.adyen_analytics}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getAdyenGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.adyen_analytics}`;
  }
}