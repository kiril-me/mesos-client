package me.kiril.mesos.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.mesos.rx.java.AwaitableSubscription;
import com.mesosphere.mesos.rx.java.Mesos4xxException;
import com.mesosphere.mesos.rx.java.Mesos5xxException;
import com.mesosphere.mesos.rx.java.MesosClientErrorContext;
import com.mesosphere.mesos.rx.java.MesosException;
import com.mesosphere.mesos.rx.java.SinkOperation;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgent;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import me.kiril.mesos.http.HttpClient;
import me.kiril.mesos.http.HttpClientRequest;
import me.kiril.mesos.http.HttpClientResponse;
import me.kiril.mesos.record.RecordIOOperator;

public class MesosClient <Send, Receive> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosClient.class);

    private static final String MESOS_STREAM_ID = "Mesos-Stream-Id";
    
    private final URI mesosUri;

    private final MessageCodec<Receive> receiveCodec;

    private Send subscribe;

    private final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor;

    private final Function<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;

    private final AtomicReference<String> mesosStreamId = new AtomicReference<>(null);
    
    private final PublishSubject<Optional<SinkOperation<Send>>> sendEvents;
    
//    private HttpClient<ByteBuf, ByteBuf> defaultHttpClient;
    
    public MesosClient(
        final String mesosUrl,
        final MessageCodec<Send> sendCodec,
        final MessageCodec<Receive> receiveCodec,
        final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor
    ) {
        this.mesosUri = URI.create("http://" + mesosUrl + "/api/v1/scheduler");
        this.receiveCodec = receiveCodec;
        this.streamProcessor = streamProcessor;
        
        sendEvents = PublishSubject.create();


        createPost = curryCreatePost(mesosUri, sendCodec, receiveCodec, mesosStreamId);
        
    }
    
    public void setSubscribe(final Send subscribe) {
        this.subscribe = subscribe;
    }

    /*
    private HttpClient<ByteBuf, ByteBuf> getHttpClient() {
        if(defaultHttpClient == null) {
            PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> pipelineConfigurator = new PipelineConfiguratorComposite<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>>(
                    new HttpClientPipelineConfigurator<ByteBuf, ByteBuf>(Integer.MAX_VALUE,
                            HttpClientPipelineConfigurator.MAX_HEADER_SIZE_DEFAULT,
                            HttpClientPipelineConfigurator.MAX_CHUNK_SIZE_DEFAULT,
                            HttpClientPipelineConfigurator.VALIDATE_HEADERS_DEFAULT,
                            HttpClientPipelineConfigurator.FAIL_ON_MISSING_RESPONSE_DEFAULT),
                    new HttpObjectAggregationConfigurator<>(Integer.MAX_VALUE));
            
            defaultHttpClient = HttpClient.<ByteBuf, ByteBuf>createClient(mesosUri.getHost(), getPort(mesosUri));
            // httpClient.setMaxConnections(50000);
            // httpClient.setIdleConnectionsTimeoutMillis(30000);
        }
        return defaultHttpClient;
    }
    */
    
    /**
     * Sends the subscribe call to Mesos and starts processing the stream of {@code Receive} events.
     * The {@code streamProcessor} function provided to the constructor will be applied to the stream of events
     * received from Mesos.
     * <p>
     * The stream processing will then process any {@link SinkOperation} that should be sent to Mesos.
     * @return The subscription representing the processing of the event stream. This subscription can then be used
     * to block the invoking thread using {@link AwaitableSubscription#await()} (For example to block a main thread
     * from exiting while events are being processed.)
     * @throws Exception 
     */
    public Disposable openStream() throws Exception {
    	final HttpClient<ByteBuf, ByteBuf> httpClient = HttpClient.<ByteBuf, ByteBuf>createClient(mesosUri.getHost(), getPort(mesosUri));
    	// httpClient.setMaxConnections(50000);
        // httpClient.setIdleConnectionsTimeoutMillis(30000);
    
        final Observable<Receive> receives = createPost.apply(subscribe)
            .flatMap(httpClient::submit)
            .subscribeOn(Schedulers.io())
            .flatMap(verifyResponseOk(subscribe, mesosStreamId, receiveCodec.mediaType()))
            .lift(new RecordIOOperator())
            .observeOn(Schedulers.computation())
            /* Begin temporary back-pressure */
            .buffer(250, TimeUnit.MILLISECONDS)
            .flatMap(Observable::fromIterable)
            /* end temporary back-pressure */
            .map(receiveCodec::decode)
            ;

        final Consumer<SinkOperation<Send>> subscriber = new SinkSubscriber<>(httpClient, createPost);
        
        final Observable<SinkOperation<Send>> sends = streamProcessor
        	.apply(receives)
        	.mergeWith(sendEvents)
            .filter(Optional::isPresent)
            .map(Optional::get);
        
        Disposable subscription = sends
            .subscribeOn(Schedulers.computation())
            .observeOn(Schedulers.io())
            .onErrorResumeNext(error -> {
            	LOGGER.error("Mesos Client error", error);
                return Observable.empty();
            })
            .buffer(1000, TimeUnit.MILLISECONDS, 5)
            .flatMap(Observable::fromIterable)
            .subscribe(subscriber);
        
        return subscription;
    }

    @NotNull
    // @VisibleForTesting
    static URI resolveRelativeUri(final @NotNull URI mesosUri, final String location) {
        final URI relativeUri = mesosUri.resolve(location);
        try {
            return new URI(
                relativeUri.getScheme(),
                relativeUri.getUserInfo(),
                relativeUri.getHost(),
                relativeUri.getPort(),
                mesosUri.getPath(),
                mesosUri.getQuery(),
                mesosUri.getFragment()
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    static String createRedirectUri(@NotNull final URI uri) {
        return uri.toString().replaceAll("/api/v1/(?:scheduler|executor)", "/redirect");
    }

    static <Send> Function<HttpClientResponse<ByteBuf>, ObservableSource<ByteBuf>> verifyResponseOk(
        @NotNull final Send subscription,
        @NotNull final AtomicReference<String> mesosStreamId,
        @NotNull final String receiveMediaType
    ) {
        return resp -> {
            final HttpResponseStatus status = resp.status();
            final int code = status.code();
            final HttpHeaders headers = resp.headers();
            
            final String contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
            
            if (code == 200 && ((receiveMediaType == null && contentType == null) || receiveMediaType.equals(contentType))) {
                if (headers.contains(MESOS_STREAM_ID)) {
                    final String streamId = headers.get(MESOS_STREAM_ID);
                    mesosStreamId.compareAndSet(null, streamId);
                }
                return resp.getContent();
            } else {
            	Observable<String> message = SinkSubscriber.readErrorResponse(resp);
            	return message.flatMap(msg -> {
            		resp.close();
            		
            		final List<Map.Entry<String, String>> entries = headers.entries();
            		final MesosClientErrorContext context = new MesosClientErrorContext(code, msg, entries);
            		if (code == 200) {
                        // this means that even though we got back a 200 it's not the sort of response we were expecting
                        // For example hitting an endpoint that returns an html document instead of a document of type
                        // `receiveMediaType`
                        throw new MesosException(
                            subscription,
                            context.withMessage(
                                String.format(
                                    "Response had Content-Type \"%s\" expected \"%s\"",
                                    contentType,
                                    receiveMediaType
                                )
                            )
                        );
                    } else if (400 <= code && code < 500) {
                        throw new Mesos4xxException(subscription, context);
                    } else if (500 <= code && code < 600) {
                        throw new Mesos5xxException(subscription, context);
                    } else {
                        LOGGER.warn("Unhandled error: context = {}", context);
                        // This shouldn't actually ever happen, but it's here for completeness of the if-else tree
                        // that always has to result in an exception being thrown so the compiler is okay with this
                        // lambda
                        throw new IllegalStateException("Unhandled error");
                    }
            	});
            }
        };
    }

    // @VisibleForTesting
    static int getPort(@NotNull final URI uri) {
        final int uriPort = uri.getPort();
        if (uriPort > 0) {
            return uriPort;
        } else {
            switch (uri.getScheme()) {
                case "http":
                    return 80;
                case "https":
                    return 443;
                default:
                    throw new IllegalArgumentException("URI Scheme must be http or https");
            }
        }
    }

    @NotNull
    // @VisibleForTesting
    static <Send, Receive> Function<Send, Observable<HttpClientRequest<ByteBuf>>> curryCreatePost(
        @NotNull final URI mesosUri,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec,
        @NotNull final AtomicReference<String> mesosStreamId
    ) {
        return (Send s) -> {
        	final byte[] bytes = sendCodec.encode(s);
        	
        	HttpClientRequest<ByteBuf> request = new HttpClientRequest<>(HttpMethod.POST, mesosUri.getPath());
        	request.withHeader(HttpHeaderNames.CONTENT_TYPE, sendCodec.mediaType());
     	    request.withHeader(HttpHeaderNames.ACCEPT, receiveCodec.mediaType());
     	   
//            HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(mesosUri.getPath())
//                .withHeader("User-Agent", userAgent.toString())
//                .withHeader("Content-Type", sendCodec.mediaType())
//                .withHeader("Accept", receiveCodec.mediaType());

            final String streamId = mesosStreamId.get();
            if (streamId != null) {
                request.withHeader(MESOS_STREAM_ID, streamId);
            }
//
//            final String userInfo = mesosUri.getUserInfo();
//            if (userInfo != null) {
//                request.withHeader(
//                    HttpHeaderNames.AUTHORIZATION.toString(),
//                    String.format("Basic %s", Base64.getEncoder().encodeToString(userInfo.getBytes()))
//                );
//            }
            
            return Observable.just(
                request
                    .withContent(bytes)
            );
        };
    }
   
   static <Send, Receive> Observable<HttpClientRequest<ByteBuf>> createPost(@NotNull URI mesosUri,
            @NotNull MessageCodec<Send> sendCodec, @NotNull MessageCodec<Receive> receiveCodec,
            @NotNull UserAgent userAgent, @NotNull AtomicReference<String> mesosStreamId, String content) {
	   
	   HttpClientRequest<ByteBuf> request = new HttpClientRequest<>(HttpMethod.POST, mesosUri.getPath());
	   request.withHeader(HttpHeaderNames.CONTENT_TYPE, "application/json");
	   request.withHeader(HttpHeaderNames.ACCEPT, "application/json");
	   
//	   
//        HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(mesosUri.getPath())
//                .withHeader("User-Agent", userAgent.toString()).withHeader("Content-Type", sendCodec.mediaType())
//                .withHeader("Accept", receiveCodec.mediaType());

        final String streamId = mesosStreamId.get();
        if (streamId != null) {
            request = request.withHeader(MESOS_STREAM_ID, streamId);
        }

        final String userInfo = mesosUri.getUserInfo();
        if (userInfo != null) {
            request = request.withHeader(HttpHeaderNames.AUTHORIZATION.toString(),
                    String.format("Basic %s", Base64.getEncoder().encodeToString(userInfo.getBytes())));
        }
        return Observable.just(request.withContent(content));
    }

    public void sendEvent(SinkOperation<Send> event) {
        sendEvents.onNext(Optional.of(event));
    }

}
