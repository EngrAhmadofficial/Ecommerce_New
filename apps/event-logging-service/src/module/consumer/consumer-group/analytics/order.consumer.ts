import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers } from "@app/common";
import { LogService } from "@app/common";

@Injectable()
export class OrderConsumer implements OnModuleInit, InitConsumers {
  private readonly logger = new Logger(OrderConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly analyticsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly logService: LogService,
  ) {
    this.noOfConsumers = this.configService.get<number>("ORDER_CONSUMERS");
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
      consumerList.push(this.startOrderConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start order consumer.
   * Consumes 'Order', 'Retailer', 'DeliveryChannel', 'Shopper' events
   * @private
   */
  private async startOrderConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getOrdersTopic()}` });
      await this.analyticsConsumer.consume(
        { topics: [this.getOrdersTopic()], fromBeginning: false },
        this.getOrdersGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.logService.createLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getOrdersTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getOrdersTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.order_analytics}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getOrdersGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.order_analytics}`;
  }
}