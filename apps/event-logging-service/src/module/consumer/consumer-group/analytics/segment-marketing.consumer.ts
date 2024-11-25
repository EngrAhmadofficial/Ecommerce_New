import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers } from "@app/common";
import { SegmentMarketingLogsService } from "@app/common";

@Injectable()
export class SegmentMarketingConsumer implements OnModuleInit, InitConsumers {
  private readonly logger = new Logger(SegmentMarketingConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly analyticsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly segmentMarketingLogsService: SegmentMarketingLogsService
  ) {
    this.noOfConsumers = this.configService.get<number>("SEGMENT_MARKETING_LOGS_CONSUMERS");
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
      consumerList.push(this.startSegmentMarketingConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start Adyen consumer.
   * Consumes 'Adyen' Events
   * @private
   */
  private async startSegmentMarketingConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getstartSegmentMarketingTopic()}` });
      await this.analyticsConsumer.consume(
        { topics: [this.getstartSegmentMarketingTopic()], fromBeginning: false },
        this.getSegmentMarketingConsumerGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.segmentMarketingLogsService.createSegmentMarketingLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getstartSegmentMarketingTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getstartSegmentMarketingTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.adyen_marketing_logs}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getSegmentMarketingConsumerGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.adyen_marketing_logs}`;
  }
}