package model

import java.util.concurrent.TimeUnit

import kafka.admin.{AdminClient => GroupClient}
import kafka.coordinator.group._
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import org.apache.kafka.common._

import collection.JavaConversions._

class KafkaTopology (groupsClient: GroupClient, adminClient: AdminClient) extends AutoCloseable {
  def createTopic(name: String, numPartitions : Int = 1, numReplicas : Short = 1): Unit = {
    val res = adminClient.createTopics(List(new NewTopic(name, numPartitions, numReplicas)))
    res.all().get()
  }

  def listTopics() : List[String] = {
    val res = adminClient.listTopics(new ListTopicsOptions().timeoutMs(300))
    res.names().get().toList
  }

  def listConsumerGroups(): Map[Node, List[GroupOverview]] = {
    groupsClient.listAllConsumerGroups();
  }

  def clearTopics(): Unit = {
    val tp = adminClient.listTopics()
    val res = adminClient.deleteTopics(tp.names().get(200, TimeUnit.MILLISECONDS))
    res.all().get()
  }

  override def close(): Unit = {
    groupsClient.close()
    adminClient.close()
  }
}
object KafkaTopology {
  def create(bootstrapServers : String): KafkaTopology = {
    val gc = GroupClient.create(Map("bootstrap.servers"->bootstrapServers))
    val ac = AdminClient.create(Map("bootstrap.servers"->bootstrapServers))
    new KafkaTopology(gc, ac)
  }
}
