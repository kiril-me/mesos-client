package me.kiril.mesos.http;

import java.util.regex.Pattern;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;

public class HttpClientResponse<T> {
	private static final Pattern PATTERN_COMMA = Pattern.compile(",");
	private static final Pattern PATTERN_EQUALS = Pattern.compile("=");
	public static final String KEEP_ALIVE_TIMEOUT_HEADER_ATTR = "timeout";

	private final HttpResponse nettyResponse;
	protected final PublishSubject<T> content;

	public HttpClientResponse(HttpResponse nettyResponse, PublishSubject<T> content) {
		this.nettyResponse = nettyResponse;
		this.content = content;
	}

	public Observable<T> getContent() {
		return content;
	}
	
	public HttpResponse original() {
		return nettyResponse;
	}
	
	public HttpResponseStatus status() {
        return nettyResponse.status();
    }

	public Long getKeepAliveTimeoutSeconds() {
		final String keepAliveHeader = nettyResponse.headers().get(HttpHeaderNames.KEEP_ALIVE);
		if (null != keepAliveHeader && !keepAliveHeader.isEmpty()) {
			String[] pairs = PATTERN_COMMA.split(keepAliveHeader);
			if (pairs != null) {
				for (String pair : pairs) {
					String[] nameValue = PATTERN_EQUALS.split(pair.trim());
					if (nameValue != null && nameValue.length >= 2) {
						String name = nameValue[0].trim().toLowerCase();
						String value = nameValue[1].trim();
						if (KEEP_ALIVE_TIMEOUT_HEADER_ATTR.equals(name)) {
							try {
								return Long.valueOf(value);
							} catch (NumberFormatException e) {
								// logger.info("Invalid HTTP keep alive timeout
								// value. Keep alive header: "
								// + keepAliveHeader + ", timeout attribute
								// value: " + nameValue[1], e);
								return null;
							}
						}
					}
				}
			}
		}
		return null;
	}

	public HttpHeaders headers() {
		return nettyResponse.headers();
	}

	public void close() {
		content.onComplete();
	}
}
