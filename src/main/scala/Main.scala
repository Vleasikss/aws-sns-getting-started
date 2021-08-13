package org.example

import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClient}
import com.amazonaws.services.sns.model.{CreateTopicRequest, CreateTopicResult, PublishRequest, SubscribeRequest, SubscribeResult}
import org.apache.log4j.PropertyConfigurator

import java.util.UUID

object Main extends Logging {

  private val TOPIC_NAME = UUID.randomUUID().toString.substring(0, 6) + "-topic"
  private val EMAIL_TOPIC_ARN = "arn:aws:sns:us-west-2:433744079960:MyTopic"
  PropertyConfigurator.configure(getClass.getResourceAsStream("/conf/log4j.properties"))

  /**
   * Creates new SNS topic
   *
   * @param amazonSNS - sns client
   * @param topicName - name of topic
   * @return result data that contains a topicArn
   */
  private def createTopic(amazonSNS: AmazonSNS, topicName: String): CreateTopicResult = {
    val request = new CreateTopicRequest(topicName)
    val response = amazonSNS.createTopic(request)
    logger.info(s"receive a response on create topic request: $response")
    response
  }

  /**
   * Sends a message by topicArn destination
   *
   * @param amazonSNS sns client
   * @param topicArn topic arn
   * @param message message to send
   * @param subject subject of message
   */
  private def sendMessage(amazonSNS: AmazonSNS, topicArn: String, message: String, subject: String): Unit = {
    val request = new PublishRequest()
      .withMessage(message)
      .withSubject(subject)
      .withTopicArn(topicArn)
      .withMessageDeduplicationId(UUID.randomUUID().toString)
    val response = amazonSNS.publish(request)
    logger.info(s"send message to a topic: $topicArn, message=$message, subject=$subject, messageId=${response.getMessageId}")
  }

  def main(args: Array[String]): Unit = {
    logger.info("Hi")
    val snsClient: AmazonSNS = AmazonSNSClient.builder().build()
    val createTopicResponse = createTopic(snsClient, TOPIC_NAME)
    val topicArn = createTopicResponse.getTopicArn

    //    val subscribeTopicRequest = new SubscribeRequest()
    //    subscribeTopicRequest.setTopicArn(EMAIL_TOPIC_ARN)
    //    subscribeTopicRequest.set
    //    val topic: SubscribeResult = snsClient.subscribe(subscribeTopicRequest)

    sendMessage(snsClient, EMAIL_TOPIC_ARN, "Hello Mail notification SNS 1", "subject1")
    sendMessage(snsClient, EMAIL_TOPIC_ARN, "Hello Mail notification SNS 2", "subject2")


  }
}
