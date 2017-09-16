package me.kiril.mesos.http;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.reactivex.Observable;
import io.reactivex.netty.channel.ByteTransformer;
import io.reactivex.netty.channel.ContentTransformer;

public class HttpClientRequest<T> {

	private Observable<T> contentSource;

	private Observable<ByteBuf> rawContentSource;

	private final HttpRequest nettyRequest;

	HttpClientRequest(HttpRequest nettyRequest) {
		this.nettyRequest = nettyRequest;
	}

	public HttpClientRequest(final HttpMethod method, final String url) {
		this.nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, url);
	}

	enum ContentSourceType {
		Raw, Typed, Absent
	}

	ContentSourceType getContentSourceType() {
		return null == contentSource ? null == rawContentSource ? ContentSourceType.Absent : ContentSourceType.Raw
				: ContentSourceType.Typed;
	}

	public HttpHeaders headers() {
		return nettyRequest.headers();
	}

	HttpRequest original() {
		return nettyRequest;
	}

	Observable<ByteBuf> getRawContentSource() {
		return rawContentSource;
	}

	Observable<T> getContentSource() {
		return contentSource;
	}

	public HttpMethod method() {
		return nettyRequest.method();
	}

	public void onWriteComplete() {
		// TODO Auto-generated method stub

	}

	public void onWriteFailed(Throwable e) {
		// TODO Auto-generated method stub

	}

	public HttpClientRequest<T> withHeader(CharSequence name, String value) {
		nettyRequest.headers().add(name, value);
		return this;
	}

	// public HttpClientRequest<T> withContent(String content) {
	// // TODO Auto-generated method stub
	// return this;
	// }
	//
	// public HttpClientRequest<T> withContent(byte[] bytes) {
	// // TODO Auto-generated method stub
	// return this;
	// }
	//
	public HttpClientRequest<T> withContent(String content) {
		return withContent(content.getBytes(Charset.defaultCharset()));
	}

	public HttpClientRequest<T> withContent(byte[] content) {
		headers().add(HttpHeaderNames.CONTENT_LENGTH, content.length);
		withRawContentSource(Observable.just(content), ByteTransformer.DEFAULT_INSTANCE);
		return this;
	}

	public <S> HttpClientRequest<T> withRawContentSource(final Observable<S> rawContentSource,
			final ContentTransformer<S> transformer) {
		this.rawContentSource = rawContentSource.map(rawContent -> transformer.call(rawContent, PooledByteBufAllocator.DEFAULT));
		return this;
	}
}
