import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers } from "@app/common";
import { SmilesTransactionService } from "@app/common";

@Injectable()
export class SmilesTransactionConsumer implements OnModuleInit, InitConsumers {
  private readonly logger = new Logger(SmilesTransactionConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly analyticsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly smilesTransService: SmilesTransactionService,
  ) {
    this.noOfConsumers = this.configService.get<number>("SMILES_TRANSACTION_CONSUMERS");
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
      consumerList.push(this.startSmilesTransactionConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start smiles transactions consumer.
   * Consumes Smiles transactional events
   * @private
   */
  private async startSmilesTransactionConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getSmilesTopic()}` });
      await this.analyticsConsumer.consume(
        { topics: [this.getSmilesTopic()], fromBeginning: false },
        this.getSmilesGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.smilesTransService.createSmilesTransactionLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getSmilesTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getSmilesTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.smiles_analytics}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getSmilesGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.smiles_analytics}`;
  }
}