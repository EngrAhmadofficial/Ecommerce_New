import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { KafkaTopicEnum } from "../../../kafka/enum/kafka-topic.enum";
import { ConsumerService } from "../../../kafka/service/consumer/consumer.service";
import { InitConsumers } from "@app/common";
import { SmilesNonTransactionService } from "@app/common";

@Injectable()
export class SmilesNonTransactionConsumer implements OnModuleInit, InitConsumers {
  private readonly logger = new Logger(SmilesNonTransactionConsumer.name);
  private noOfConsumers: number;

  constructor(
    private readonly analyticsConsumer: ConsumerService,
    private readonly configService: ConfigService,
    private readonly smilesNonTransService: SmilesNonTransactionService,
  ) {
    this.noOfConsumers = this.configService.get<number>("SMILES_NON_TRANSACTION_CONSUMERS");
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
      consumerList.push(this.startSmilesNonTransactionConsumer(i + 1));
    }
    await Promise.all(consumerList);
  }

  /**
   * Method to Start smiles non transactions consumer.
   * Consumes Smiles non transactional events
   * @private
   */
  private async startSmilesNonTransactionConsumer(consumerNo: number): Promise<void> {
    try {
      this.logger.log({ message: `Starting consumer no. ${consumerNo} for topic: ${this.getSmilesNonTransTopic()}` });
      await this.analyticsConsumer.consume(
        { topics: [this.getSmilesNonTransTopic()], fromBeginning: true },
        this.getSmilesNonTransGroupName(),
        {
          eachMessage: async ({ topic, partition, message }) => {
            await this.smilesNonTransService.createSmilesNonTransactionLog(message.value.toString());
          }
        }
      );
    } catch (e) {
      this.logger.error({ message: `Error starting consumer no. ${consumerNo} for topic: ${this.getSmilesNonTransTopic()}` }, e);
    }
  }

  /**
   * Method to get consumer topic.
   * @private
   */
  private getSmilesNonTransTopic(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_TOPIC_PREFIX")}${KafkaTopicEnum.smiles_non_transactional}`;
  }

  /**
   * Method to get consumer group.
   * @private
   */
  private getSmilesNonTransGroupName(): string {
    return `${this.configService.get<string>("CLOUDKARAFKA_GROUPID_PREFIX")}${KafkaTopicEnum.smiles_non_transactional}`;
  }
}