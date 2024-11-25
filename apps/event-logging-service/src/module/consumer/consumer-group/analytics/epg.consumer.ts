import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { EpgLogsService, InitConsumers } from "@app/common";

@Injectable()
export class EpgConsumer implements OnModuleInit, InitConsumers {
  private readonly logger: Logger = new Logger(EpgConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly epgConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly epgLogsService: EpgLogsService
  ) {
    this.noOfConsumers = this.configService.get<number>("EPG_CONSUMERS");
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
      consumerList.push(this.startEpgConsumers(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to start epg transactions requests consumer.
   * Consumes Epg Transaction Request events
   * @private
   */
  private async startEpgConsumers(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getEpgTopic()}` });
      await this.epgConsumer.consume(
        { topics: [this.getEpgTopic()], fromBeginning: true },
        this.getEpgGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.epgLogsService.createEpgLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getEpgTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getEpgTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.epg_logs}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getEpgGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.epg_logs}`;
  }
}