package model

import org.scalatest._

trait KafkaTopologyBuilder extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  var topology : KafkaTopology = null

  override def beforeAll(): Unit = {
    try super.beforeAll()
    try topology = KafkaTopology.create("localhost:9092")
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally topology.close()
  }

  override def beforeEach(): Unit = {
    try super.beforeEach()
    topology.clearTopics()
  }
}

class KafkaTopologySpec extends FlatSpec with KafkaTopologyBuilder {

  "A KafkaTopology" should "have no topics when new" in {
    assert(topology.listTopics().isEmpty)
  }

  it should "return a topic after creating it" in {
    topology.createTopic("test-topic")
    assert(topology.listTopics().length == 1)
  }
}
