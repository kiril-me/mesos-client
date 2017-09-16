package me.kiril.mesos.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;

public class ClientRequestResponseConverter<O> extends ChannelDuplexHandler {

	private final PublishSubject<O> contentSubject;

	public ClientRequestResponseConverter() {
		contentSubject = PublishSubject.create();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		final Class<?> recievedMsgClass = msg.getClass();

		if (HttpResponse.class.isAssignableFrom(recievedMsgClass)) {
			HttpResponse response = (HttpResponse) msg;
			DecoderResult decoderResult = response.decoderResult();
			if (decoderResult.isFailure()) {
				final Throwable error = decoderResult.cause();
				//
				contentSubject.onError(error);

			} else {
				HttpClientResponse<O> rxResponse = new HttpClientResponse<O>(response, contentSubject);
				super.channelRead(ctx, rxResponse);
			}
		}

		if (HttpContent.class.isAssignableFrom(recievedMsgClass)) {
			ByteBuf content = ((ByteBufHolder) msg).content();

			if (LastHttpContent.class.isAssignableFrom(recievedMsgClass)) {
				if (content.isReadable()) {
					invokeContentOnNext(content);
				} else {
					ReferenceCountUtil.release(content);
				}

				// if (null != requestProcessingObserver) {
				// requestProcessingObserver.onComplete();
				// }
				// requestProcessingObserver.onCom
				contentSubject.onComplete();
			} else {
				invokeContentOnNext(content);
			}
		} else if (!HttpResponse.class.isAssignableFrom(recievedMsgClass)) {
			invokeContentOnNext(msg);
		}
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		Class<?> recievedMsgClass = msg.getClass();

		if (HttpClientRequest.class.isAssignableFrom(recievedMsgClass)) {
			final HttpClientRequest<?> rxRequest = (HttpClientRequest<?>) msg;
			final HttpHeaders headers = rxRequest.headers();
			final boolean isNotContentLengthSet = !HttpUtil.isContentLengthSet(rxRequest.original());
			Observable<?> contentSource = null;

			switch (rxRequest.getContentSourceType()) {
			case Raw:
				if (isNotContentLengthSet) {
					headers.add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
				}
				contentSource = rxRequest.getRawContentSource();
				break;
			case Typed:
				if (isNotContentLengthSet) {
					headers.add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
				}
				contentSource = rxRequest.getContentSource();
				break;
			case Absent:
				if (isNotContentLengthSet && rxRequest.method() != HttpMethod.GET) {
					headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
				}
				break;
			}

			ctx.write(rxRequest.original());

			if (null != contentSource) {
				if (isNotContentLengthSet) {
					headers.add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
				}
				writeContent(ctx, contentSource, promise, rxRequest);
			} else { // If no content then write Last Content immediately.
				// In order for netty's codec to understand that HTTP request
				// writing is over, we always have to write the
				// LastHttpContent irrespective of whether it is chunked or not.
				writeLastHttpContent(ctx, rxRequest);
			}

		} else {
			ctx.write(msg, promise); // pass through, since we do not understand
										// this message.
		}
		
		
	}

	private void writeContent(ChannelHandlerContext ctx, Observable<?> contentSource, ChannelPromise promise,
			HttpClientRequest<?> rxRequest) {
		contentSource.subscribe(new Observer<Object>() {
			@Override
			public void onComplete() {
				writeLastHttpContent(ctx, rxRequest);
			}

			@Override
			public void onError(Throwable e) {
				promise.tryFailure(e);
				rxRequest.onWriteFailed(e);
			}

			@Override
			public void onNext(Object chunk) {
				writeAContentChunk(ctx, chunk);
			}

			@Override
			public void onSubscribe(Disposable d) {
				// TODO Auto-generated method stub
			}		
		});
	}

	private void writeLastHttpContent(ChannelHandlerContext ctx, HttpClientRequest<?> rxRequest) {
		writeAContentChunk(ctx, new DefaultLastHttpContent());
		rxRequest.onWriteComplete();
	}

	private ChannelFuture writeAContentChunk(ChannelHandlerContext ctx, Object chunk) {
		return ctx.write(chunk);
	}


	@SuppressWarnings("unchecked")
	private void invokeContentOnNext(Object nextObject) {
		try {
			contentSubject.onNext((O) nextObject);
		} catch (ClassCastException e) {
			contentSubject.onError(e);
		} finally {
			ReferenceCountUtil.release(nextObject);
		}
	}

}
