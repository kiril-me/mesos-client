package me.kiril.mesos.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.Observer;

public class ObservableAdapter<I> extends ChannelInboundHandlerAdapter {

	private Observer<HttpClientResponse<I>> bridgedObserver;

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (null != bridgedObserver) {
			try {
				bridgedObserver.onNext((HttpClientResponse<I>) msg);
			} catch (ClassCastException cce) {
				bridgedObserver.onError(new RuntimeException("Mismatched message type.", cce));
			} finally {
				ReferenceCountUtil.release(msg);
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (null != bridgedObserver) {
			bridgedObserver.onError(cause);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (null != bridgedObserver) {
			bridgedObserver.onComplete();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
		if(event instanceof Observer) {
			bridgedObserver = (Observer<HttpClientResponse<I>>) event;
		}
		super.userEventTriggered(ctx, event);
	}
}
