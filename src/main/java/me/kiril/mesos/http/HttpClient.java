package me.kiril.mesos.http;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Base64;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

public class HttpClient<I, O> {

	private SocketAddress address;

	private final Bootstrap bootstrap;

	private final EventLoopGroup group;

	private final HttpVersion version = HttpVersion.HTTP_1_1;

	private final String userAgent = "mesos-http-client";

	private final boolean compress = true;
	
	private String userInfo;

	public static <I, O> HttpClient<I, O> createClient(String hostname, int port) {
		final InetSocketAddress address = new InetSocketAddress(hostname, port);
		final EventLoopGroup group = new NioEventLoopGroup();
		final Class<? extends SocketChannel> socketClass = NioSocketChannel.class; // EpollSocketChannel.class;
		final HttpClient<I, O> client = new HttpClient<>(address, group, socketClass);

		return client;
	}

	private HttpClient(final SocketAddress address, final EventLoopGroup group,
			final Class<? extends SocketChannel> socketClass) {
		this.address = address;
		this.group = group;

		bootstrap = new Bootstrap();
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
		bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		bootstrap.group(group).channel(socketClass)
				// .handler(new HttpRequestHandler());

				.handler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(final SocketChannel ch) throws Exception {
						final ChannelPipeline pipeline = ch.pipeline();
						pipeline.addLast("http-codec", new HttpClientCodec());
						pipeline.addLast("decompressor", new HttpContentDecompressor());
						pipeline.addLast("request-response", new ClientRequestResponseConverter<O>());
						pipeline.addLast("observable-adapter", new ObservableAdapter<O>());
						
//						ch.pipeline().addLast(new HttpObjectAggregator(1048576));
						// ch.pipeline().addLast(new
						// SimpleChannelInboundHandler<FullHttpResponse>() {
						// @Override
						// protected void channelRead0(ChannelHandlerContext
						// ctx, FullHttpResponse msg)
						// throws Exception {
						// final String echo =
						// msg.content().toString(CharsetUtil.UTF_8);
						// System.out.println("Response: " + echo);
						// }
						// });

//						pipeline.addLast("http-response", new HttpResponseHandler());
						// pipeline.addLast("http-request", new
						// HttpRequestHandler());
						// pipeline.addLast("http-aggegator", new
						// HttpObjectAggregator(512 * 1024));
						// pipeline.addLast(new
						// HttpObjectAggregator(Integer.MAX_VALUE));
					}

				});

		// .handler(new SimpleChannelInboundHandler<ByteBuf>() {
		//// @Override
		//// public void channelRegistered(ChannelHandlerContext ctx)
		//// throws Exception {
		//// Integer idValue = ctx.channel().attr(id).get();
		//// }
		// @Override
		// protected void channelRead0(ChannelHandlerContext ctx, ByteBuf
		// byteBuf) throws Exception {
		// System.out.println("Reveived data");
		// byteBuf.clear();
		// }
		// });
	}

	public HttpClientRequest<I> createGet(String url) {
		return createRequest(HttpMethod.GET, url);
	}

	public HttpClientRequest<I> createPost(String url) {
		return createRequest(HttpMethod.POST, url);
	}

	private HttpClientRequest<I> createRequest(HttpMethod method, String url) {
		HttpRequest request = new DefaultHttpRequest(version, method, url);
		return new HttpClientRequest<I>(request);
	}
	
	
	

	public Observable<HttpClientResponse<I>> submit(HttpClientRequest<I> request) {
		Observable<HttpClientResponse<I>> requestObservable = Observable
				.just(request)
				.map(requestUrl -> {
//					HttpRequest request = new DefaultHttpRequest(version, HttpMethod.POST, requestUrl);
					HttpHeaders headers = request.headers();
					headers.add(HttpHeaderNames.USER_AGENT, userAgent);
					if (compress) {
						headers.add(HttpHeaderNames.ACCEPT_ENCODING, "gzip");
					}
//					headers.add(HttpHeaderNames.CONTENT_TYPE, "application/json");
//					headers.add(HttpHeaderNames.ACCEPT, "application/json");
					if(userInfo != null) {
						String encodedUserInfo = Base64.getEncoder().encodeToString(userInfo.getBytes());
						headers.add(HttpHeaderNames.AUTHORIZATION, String.format("Basic %s", encodedUserInfo));
					}

					return request;
				})
				.flatMap(this::submitRequest);
//				.flatMap(response -> response.content);
//				.switchMap(cf -> )
//				.doOnNext(channel -> channel.syncUninterruptibly())
//				.map(chanel -> (O) null);
		
		return requestObservable;
	}
	
	private Observable<HttpClientResponse<I>> submitRequest(HttpClientRequest<I> request) {
		final Subject<HttpClientResponse<I>> inputSubject = PublishSubject.<HttpClientResponse<I>>create().toSerialized();
		final ChannelFuture cf = bootstrap.connect(address);
		cf.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					final Channel channel = future.channel();
					if(channel.isWritable()) {
						ChannelHandlerContext firstContext = channel.pipeline().firstContext();
				        firstContext.fireUserEventTriggered(inputSubject);
				        
						future = channel.writeAndFlush(request);
					}
				} else {
					Throwable error = future.cause();
					System.out.println("Error: " + error);
					inputSubject.onError(error);
				}
			}
			
		});
		return inputSubject;
	}
	
	public void close() {
		group.shutdownGracefully();
	}

	public static void main(String[] args) {
		HttpClient<ByteBuf, ByteBuf> client = HttpClient.createClient("10.8.0.1", 5050);
		HttpClientRequest<ByteBuf> request = client.createPost("/api/v1/scheduler");
		client.submit(request).subscribe(System.out::println); // "/master/state.json");// 
		//client.close();
	}

}
