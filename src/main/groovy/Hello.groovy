import com.hazelcast.config.Config
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Immutable
import io.netty.util.CharsetUtil
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageCodec
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

/**
 * Created by kowalzik on 05.01.2017.
 */
class Hello {
    static void main(args) {
        def hazelcastConfig = new Config()
        hazelcastConfig.instanceName = "King Bob Cluster"
        println("Starting Clustermanager...")
        def mgr = new HazelcastClusterManager(hazelcastConfig)
        def options = new VertxOptions().setClusterManager(mgr)
        Vertx.clusteredVertx(options) { res ->
            if (res.succeeded()) {
                println("Cluster started")
                def vertx = res.result()
                def eventBus = vertx.eventBus()
                println("We now have a clustered event bus: " + eventBus)
                eventBus.registerCodec(new RequestMessageCodec())

                vertx.deployVerticle(MyFirstVerticle.class.name)
                vertx.deployVerticle(TimerVerticle.class.name)
            } else {
                // failed!
                println("Clusterstartup failed ${res.cause()}")
            }
        }
    }
}

class RequestMessageCodec implements MessageCodec<RequestMessage, RequestMessage> {

    def jsonSlurper = new JsonSlurper()

    String name() {
        return "RequestMessageCodec"
    }

    byte systemCodecID() {
        return -1
    }

    RequestMessage transform(RequestMessage input) {
        return new RequestMessage(input.msgId, input.host, input.uri, input.port)
    }

    void encodeToWire(Buffer buffer, RequestMessage requestMessage) {
        def bytes = JsonOutput.toJson(requestMessage).getBytes(CharsetUtil.UTF_8)
        buffer.appendInt(bytes.length)
        buffer.appendBytes(bytes)
    }

    RequestMessage decodeFromWire(int pos, Buffer buffer) {
        def length = buffer.getInt(pos)
        def startPayload = pos + 4
        def bytes = buffer.getBytes(startPayload, startPayload + length)
        return jsonSlurper.parse(bytes, CharsetUtil.UTF_8)
    }
}

@Immutable
class RequestMessage {
    String msgId, host, uri
    int port
}

class MyFirstVerticle extends AbstractVerticle {

    void start(Future<Void> future) {
        def eventBus = vertx.eventBus()
        eventBus.consumer("send.request", { it -> receiveRequest(it) })
    }

    def receiveRequest(Message<?> message) {
        println("I have received a message: ${message.body()}")

        def theRequest = message.body()
        if (theRequest instanceof RequestMessage) {
            def start = System.currentTimeMillis()
            println("Start=$start")
            vertx.createHttpClient()
                    .getNow(theRequest.port, theRequest.host, theRequest.uri, { response -> println("Received response with status code ${response.statusCode()} and took ${System.currentTimeMillis() - start}ms") })
        }
    }

}

class TimerVerticle extends AbstractVerticle {

    void start(Future<Void> future) {
        vertx.setPeriodic(10000, { it -> sendMessage() })
    }

    def sendMessage() {
        def eventBus = vertx.eventBus()
        def requestMessage = new RequestMessage(msgId: "Groovy-${UUID.randomUUID().toString()}", host: "www.golem.de", port: 80, uri: "/")
        println(requestMessage)
        eventBus.send("send.request", requestMessage, new DeliveryOptions().setCodecName("RequestMessageCodec"))
    }
}
