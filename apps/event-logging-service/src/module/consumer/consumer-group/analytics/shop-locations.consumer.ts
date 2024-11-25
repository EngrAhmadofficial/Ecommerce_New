import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers, ShopLocationsLogsService } from "@app/common";

@Injectable()
export class ShopLocationsConsumer implements OnModuleInit, InitConsumers {
  private readonly logger: Logger = new Logger(ShopLocationsConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly shopLocationsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly shopLocationsLogsService: ShopLocationsLogsService,
  ) {
    this.noOfConsumers = this.configService.get<number>("SHOP_LOCATIONS_CONSUMERS");
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
      consumerList.push(this.startShopLocationsConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start shop location requests consumer.
   * Consumes Shop Location Request events
   * @private
   */
  private async startShopLocationsConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getShopLocationsTopic()}` });
      await this.shopLocationsConsumer.consume(
        { topics: [this.getShopLocationsTopic()], fromBeginning: false },
        this.getShopLocationsGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.shopLocationsLogsService.createShopLocationsLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getShopLocationsTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getShopLocationsTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.shop_locations}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getShopLocationsGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.shop_locations}`;
  }
}