package org.example

import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClient}
import com.amazonaws.services.sns.model.{CreateTopicRequest, CreateTopicResult, PublishRequest}
import org.apache.log4j.PropertyConfigurator

import java.util.{Properties, UUID}

object Main extends Logging {

  PropertyConfigurator.configure(getClass.getResourceAsStream("/conf/log4j.properties"))

  private val DEFAULT_TOPIC_NAME = UUID.randomUUID().toString.substring(0, 6) + "-topic"
  val properties = new Properties()
  properties.load(getClass.getResourceAsStream("/application.properties"))

  private val EMAIL_TOPIC_ARN = properties.getProperty("sns.topic.email.arn")
  private val SMS_TOPIC_ARN = properties.getProperty("sns.topic.sms.arn")
  private val PHONE_NUMBER = properties.getProperty("sns.topic.sms.phoneNumber")
  private val SQS_QUEUE_NAME = properties.getProperty("sqs.queue.name")

  /**
   * Creates new SNS topic
   *
   * @param amazonSNS - sns client
   * @param topicName - name of topic
   * @return result data that contains a topicArn
   */
  private def createTopic(amazonSNS: AmazonSNS, topicName: String = DEFAULT_TOPIC_NAME): CreateTopicResult = {
    val request = new CreateTopicRequest(topicName)
    val response = amazonSNS.createTopic(request)
    logger.info(s"receive a response on create topic request: $response")
    response
  }

  /**
   * Sends a message by topicArn destination
   *
   * @param amazonSNS sns client
   * @param topicArn  topic arn
   * @param message   message to send
   * @param subject   subject of message
   */
  private def sendMailMessage(amazonSNS: AmazonSNS, topicArn: String, message: String, subject: String): Unit = {
    val request = new PublishRequest()
      .withMessage(message)
      .withSubject(subject)
      .withTopicArn(topicArn)
    val response = amazonSNS.publish(request)
    logger.info(s"send message to a topic: $topicArn, message=$message, subject=$subject, messageId=${response.getMessageId}")
  }

  /**
   * Sends a SMS message by a phone number
   *
   * It allows to send notifications only by its region
   *
   * @param amazonSNS   amazon sns client
   * @param topicArn    arn topic
   * @param message     text message
   * @param phoneNumber phone number to send by
   */
  private def sendPhoneNumberMessage(amazonSNS: AmazonSNS, topicArn: String, message: String, phoneNumber: String): Unit = {
    val request = new PublishRequest()
      .withMessage(message)
      .withPhoneNumber(phoneNumber)
    val response = amazonSNS.publish(request)
    logger.info(s"send message to a topic: $topicArn, message=$message, phoneNumber=$phoneNumber, messageId=${response.getMessageId}")
  }

  def main(args: Array[String]): Unit = {
    logger.info("Hi")
    val snsClient: AmazonSNS = AmazonSNSClient.builder().build()
    //    val createTopicResponse = createTopic(snsClient, TOPIC_NAME)
    //    val topicArn = createTopicResponse.getTopicArn

    //    val subscribeTopicRequest = new SubscribeRequest()
    //    subscribeTopicRequest.setTopicArn(EMAIL_TOPIC_ARN)
    //    subscribeTopicRequest.set
    //    val topic: SubscribeResult = snsClient.subscribe(subscribeTopicRequest)

    sendMailMessage(snsClient, EMAIL_TOPIC_ARN, "Hello Mail notification SNS 1", "subject1")
    sendPhoneNumberMessage(snsClient, SMS_TOPIC_ARN, "Hello Mail notification SNS 2", PHONE_NUMBER)


  }
}
