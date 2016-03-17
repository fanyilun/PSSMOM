package fyl.middleware.mom.consumer;


import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MessageListener;
import fyl.middleware.mom.encode.MyDecoder;
import fyl.middleware.mom.encode.MyEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
/**
 * 
 * @author yilun.fyl
 */
public class ConsumerConnection{
	
	/**
	 * broker的通讯端口 固定9999
	 */
    public static final int PORT = 9999;
    
    /**
     * 发送者类型，随properties发送
     */
    public static final String TYPE="consumer";
    
    /**
     * 连接是否已经主动关闭
     */
    private boolean permitClose;
    
    
    private String brokerIp;
    
    private Channel channel;
    
    
    public ConsumerConnection(String brokerIp) {
    	this.brokerIp = brokerIp;
	}

    /**
     * 与服务器建立连接
     * @param subscribeMsg
     * @param listener
     * @throws Throwable
     */
    public void connect(final MessageExt subscribeMsg,  final MessageListener listener) throws Throwable{
    	permitClose = false;
        EventLoopGroup group = new NioEventLoopGroup();
        try{
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class).remoteAddress(brokerIp,PORT)
                    .option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new IdleStateHandler(0,5,0));	//用于发送心跳包
                    socketChannel.pipeline().addLast(new MyDecoder());
                    socketChannel.pipeline().addLast(new MyEncoder());
                    socketChannel.pipeline().addLast(new ConsumerConnectionHandler(listener,subscribeMsg,ConsumerConnection.this));
                }
            });
            
            ChannelFuture channelFuture = bootstrap.connect().sync();
            if (!channelFuture.awaitUninterruptibly().isSuccess()){
                throw new Exception("connect fail");
            }
            channel = channelFuture.channel();
        } catch (InterruptedException e) {
            throw new Exception("connect fail");
        }

    }

    /**
     * 消费者主动关闭连接
     */
    public void close(){
        if (null != channel) {
            permitClose=true;
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

	public boolean isPermitClose() {
		return permitClose;
	}

   
}
