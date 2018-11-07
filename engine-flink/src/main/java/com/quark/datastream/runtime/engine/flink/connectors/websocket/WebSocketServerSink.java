package com.quark.datastream.runtime.engine.flink.connectors.websocket;

import com.quark.datastream.runtime.task.DataSet;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketServerSink extends RichSinkFunction<DataSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerSink.class);

  private final int port;

  private EventLoopGroup bossGroup = null;
  private EventLoopGroup workerGroup = null;

  private Channel serverChannel = null;
  private ConcurrentMap<String, Channel> connectedClients = null;

  public WebSocketServerSink(int port) {
    this.port = port;
    this.connectedClients = new ConcurrentHashMap<>();
  }

  @Override
  public void invoke(DataSet dataSet) throws Exception {
    Collection<Channel> channels = connectedClients.values();
    for (Channel channel : channels) {
      StringBuilder sb = new StringBuilder();
      // if (dataSet.isBatch()) {
      //     for (DataSet.Record entry : dataSet.getRecords()) {
      //         sb.append(entry.toString()).append("\n");
      //     }
      //     sb.deleteCharAt(sb.length() - 1);
      // } else {
      //     sb.append(dataSet.getStreamedRecord().toString());
      // }
      LOGGER.info("Writing to {}. DataSet: {}", channel.localAddress().toString(),
          dataSet.toString());
      sb.append(dataSet.toString());
      TextWebSocketFrame webSocketFrame = new TextWebSocketFrame(sb.toString());
      channel.writeAndFlush(webSocketFrame);
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // start web server
    this.bossGroup = new NioEventLoopGroup(1);
    this.workerGroup = new NioEventLoopGroup();

    ServerBootstrap b = new ServerBootstrap();
    b.option(ChannelOption.SO_BACKLOG, 1024);
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new HttpInitializer());

    LOGGER.info("Starting web socket server on port {}", this.port);
    this.serverChannel = b.bind(this.port).sync().channel();
  }

  @Override
  public void close() throws Exception {
    // close web server

    if (this.serverChannel != null) {
      this.serverChannel.close();
    }
    if (this.bossGroup != null) {
      this.bossGroup.shutdownGracefully();
    }
    if (this.workerGroup != null) {
      this.workerGroup.shutdownGracefully();
    }

    LOGGER.info("Web socket server terminated.");

    super.close();
  }

  private class HttpInitializer extends ChannelInitializer<SocketChannel> {

    protected void initChannel(SocketChannel socketChannel) throws Exception {
      ChannelPipeline pipeline = socketChannel.pipeline();
      pipeline.addLast("httpServerCodec", new HttpServerCodec());
      pipeline.addLast("httpHandler", new HttpServerHandler());
    }
  }

  private class HttpServerHandler extends ChannelInboundHandlerAdapter {

    private WebSocketServerHandshaker handshaker;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);

      connectedClients.put(ctx.channel().toString(), ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      connectedClients.remove(ctx.channel().toString());

      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof HttpRequest && !(msg instanceof FullHttpRequest)) {
        HttpRequest httpRequest = (HttpRequest) msg;
        HttpHeaders headers = httpRequest.headers();
        if (headers.get("Connection").equalsIgnoreCase("Upgrade")
            || headers.get("Upgrade").equalsIgnoreCase("WebSocket")) {
          //Adding new handler to the existing pipeline to handle WebSocket Messages
          ctx.pipeline().replace(this, "websocketHandler", new WebSocketHandler());

          //Do the Handshake to upgrade connection from HTTP to WebSocket protocol
          handleHandshake(ctx, httpRequest);
        }
      }
    }

    /* Do the handshaking for WebSocket request */
    protected void handleHandshake(ChannelHandlerContext ctx, HttpRequest req)
        throws URISyntaxException {
      WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
          getWebSocketUrl(req),
          null, true);
      handshaker = wsFactory.newHandshaker(req);
      if (handshaker == null) {
        WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
      } else {
        handshaker.handshake(ctx.channel(), req);
      }
    }

    protected String getWebSocketUrl(HttpRequest req) {
      LOGGER.info("Req URI : " + req.getUri());
      String url = "ws://" + req.headers().get("Host") + req.getUri();
      LOGGER.info("Constructed URL : " + url);
      return url;
    }

  }

  private class WebSocketHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

      if (msg instanceof WebSocketFrame) {
        LOGGER.debug("This is a WebSocket frame");
        LOGGER.debug("Client Channel : " + ctx.channel());
        if (msg instanceof CloseWebSocketFrame) {
          LOGGER.debug("CloseWebSocketFrame Received : ");
          LOGGER.debug("ReasonText :" + ((CloseWebSocketFrame) msg).reasonText());
          LOGGER.debug("StatusCode : " + ((CloseWebSocketFrame) msg).statusCode());

          ctx.channel().close(); // close client socket from server
          connectedClients.remove(ctx.channel().toString());
        } else {
          LOGGER.debug("Unsupported WebSocketFrame");
        }
      }
    }
  }


}
