package me.kiril.mesos.client;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.mesosphere.mesos.rx.java.Mesos4xxException;
import com.mesosphere.mesos.rx.java.Mesos5xxException;
import com.mesosphere.mesos.rx.java.MesosClientErrorContext;
import com.mesosphere.mesos.rx.java.MesosException;
import com.mesosphere.mesos.rx.java.SinkOperation;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import me.kiril.mesos.http.HttpClient;
import me.kiril.mesos.http.HttpClientRequest;
import me.kiril.mesos.http.HttpClientResponse;

public class SinkSubscriber<Send> implements Consumer<SinkOperation<Send>> {
	
	private final HttpClient<ByteBuf, ByteBuf> httpClient;
	
	private final Function<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;
	
	public SinkSubscriber(HttpClient<ByteBuf, ByteBuf> httpClient, Function<Send, Observable<HttpClientRequest<ByteBuf>>> createPost) {
		this.httpClient = httpClient;
		this.createPost = createPost;
	}
	
	@Override
	public void accept(final SinkOperation<Send> op) throws Exception {
		final Send toSink = op.getThingToSink();
		
		createPost.apply(toSink)
			.flatMap(httpClient::submit)
			.flatMap(resp -> {
				final HttpResponseStatus status = resp.status();
				final int code = status.code();
				if(code == 202) {
					return Observable.just(Optional.<MesosException>empty());
				} else {
					final HttpHeaders headers = resp.headers();
					Observable<String> message = SinkSubscriber.readErrorResponse(resp);
					return message.map(msg -> {
						resp.close();
	            		
	            		final List<Map.Entry<String, String>> entries = headers.entries();
	            		final MesosClientErrorContext context = new MesosClientErrorContext(code, msg, entries);
	            		MesosException error;
                        if (400 <= code && code < 500) {
                            // client error
                            error = new Mesos4xxException(toSink, context);
                        } else if (500 <= code && code < 600) {
                            // client error
                            error = new Mesos5xxException(toSink, context);
                        } else {
                            // something else that isn't success but not an error as far as http is concerned
                            error = new MesosException(toSink, context);
                        }
                        return Optional.of(error);
					});
				}
			})
            .observeOn(Schedulers.computation())
            .subscribe(exception -> {
                if (!exception.isPresent()) {
                    op.onCompleted();
                } else {
                    op.onError(exception.get());
                }
            });
	}

	public static Observable<String> readErrorResponse(final HttpClientResponse<ByteBuf> resp) {
		Observable<String> message = null;
		final HttpHeaders headers = resp.headers();
		Integer contentLength = headers.getInt(HttpHeaderNames.CONTENT_LENGTH);
		if (contentLength != null && contentLength > 0) {
			final String contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
			if (contentType == null) {
				message = Observable.just("Not attempting to decode error response with unspecified Content-Type");
			} else if (contentType.startsWith("text/plain")) {
				message = resp.getContent().map(r -> r.toString(StandardCharsets.UTF_8));
			} else {
				message = Observable.just(
						String.format("Not attempting to decode error response of type '%s' as string", contentType));
			}
		}
		if (message == null) {
			message = Observable.just("");
		}
		return message;
	}
}
