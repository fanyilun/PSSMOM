package fyl.middleware.mom.broker;

import fyl.middleware.mom.encode.MyDecoder;
import fyl.middleware.mom.encode.MyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 消息服务器Broker
 */
public class Broker {
	
    private static final int PORT = 9999;
    private RegistService registserver=new RegistService();
    
    
    public Broker() {
    }

    public void start(){

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try{
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup,workerGroup).channel(NioServerSocketChannel.class)
                    .localAddress(PORT).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new MyDecoder());
                    socketChannel.pipeline().addLast(new MyEncoder());
                    socketChannel.pipeline().addLast(new MomServerHandler(registserver));
                }
            }).option(ChannelOption.SO_BACKLOG, 1024)
                                    .option(ChannelOption.SO_REUSEADDR, true)
                                    .option(ChannelOption.TCP_NODELAY, true)
                                    .option(ChannelOption.SO_SNDBUF, 65536)
                                    .option(ChannelOption.SO_RCVBUF,65536);

            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            System.out.println("MOM Server starts successfully!!");
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) {
        Broker broker=new Broker();
        broker.start();
    }
}
