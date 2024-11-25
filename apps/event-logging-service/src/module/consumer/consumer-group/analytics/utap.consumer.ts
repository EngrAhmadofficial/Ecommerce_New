import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { UtapLogsService, InitConsumers } from "@app/common";

@Injectable()
export class UtapConsumer implements OnModuleInit, InitConsumers {
  private readonly logger: Logger = new Logger(UtapConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly utapConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly utapLogsService: UtapLogsService
  ) {
    this.noOfConsumers = this.configService.get<number>("UTAP_CONSUMERS");
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
      consumerList.push(this.startUtapConsumers(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to start utap transactions requests consumer.
   * Consumes Utap Transaction Request events
   * @private
   */
  private async startUtapConsumers(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getUTapTopic()}` });
      await this.utapConsumer.consume(
        { topics: [this.getUTapTopic()], fromBeginning: true },
        this.getUtapGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.utapLogsService.createUtapLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getUTapTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getUTapTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.utap_logs}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getUtapGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.utap_logs}`;
  }
}