package me.kiril.mesos.record;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;

public class RecordIOOperator implements ObservableOperator<byte[], ByteBuf> {

	@Override
	public Observer<? super ByteBuf> apply(Observer<? super byte[]> observer) throws Exception {
		return new RecordIOSubscriber(observer);
	}

	private static class RecordIOSubscriber implements Observer<ByteBuf>, Disposable {
		private static final Logger LOGGER = LoggerFactory.getLogger(RecordIOSubscriber.class);

		final Observer<? super byte[]> child;
		final List<Byte> messageSizeBytesBuffer = new ArrayList<>();
		boolean allSizeBytesBuffered = false;
		byte[] messageBytes = null;
		int remainingBytesForMessage = 0;

		Disposable d;

		public RecordIOSubscriber(Observer<? super byte[]> observer) {
			this.child = observer;
		}

		@Override
		public void onSubscribe(Disposable d) {
			if (DisposableHelper.validate(this.d, d)) {
				this.d = d;

				child.onSubscribe(d);
			}
		}

		@Override
		public void onNext(ByteBuf t) {
			try {
				final ByteBufInputStream in = new ByteBufInputStream(t);
				while (t.readableBytes() > 0) {
					// New message
					if (remainingBytesForMessage == 0) {

						// Figure out the size of the message
						byte b;
						while ((b = (byte) in.read()) != -1) {
							if (b == (byte) '\n') {
								allSizeBytesBuffered = true;
								break;
							} else {
								messageSizeBytesBuffer.add(b);
							}
						}

						// Allocate the byte[] for the message and get ready to
						// read it
						if (allSizeBytesBuffered) {
							final byte[] bytes = getByteArray(messageSizeBytesBuffer);
							allSizeBytesBuffered = false;
							final String sizeString = new String(bytes, StandardCharsets.UTF_8);
							messageSizeBytesBuffer.clear();
							final long l = Long.valueOf(sizeString, 10);
							if (l > Integer.MAX_VALUE) {
								LOGGER.warn(
										"specified message size ({}) is larger than Integer.MAX_VALUE. Value will be truncated to int");
								remainingBytesForMessage = Integer.MAX_VALUE;
								// TODO: Possibly make this more robust to
								// account for things larger than 2g
							} else {
								remainingBytesForMessage = (int) l;
							}

							messageBytes = new byte[remainingBytesForMessage];
						}
					}

					// read bytes until we either reach the end of the ByteBuf
					// or the message is fully read.
					final int readableBytes = t.readableBytes();
					if (readableBytes > 0) {
						final int writeStart = messageBytes.length - remainingBytesForMessage;
						final int numBytesToCopy = Math.min(readableBytes, remainingBytesForMessage);
						final int read = in.read(messageBytes, writeStart, numBytesToCopy);
						remainingBytesForMessage -= read;
					}

					// Once we've got a full message send it on downstream.
					if (remainingBytesForMessage == 0 && messageBytes != null) {
						child.onNext(messageBytes);
						messageBytes = null;
					}
				}
			} catch (Exception e) {
				onError(e);
			}
		}

		@Override
		public void onError(Throwable e) {
			child.onError(e);
		}

		@Override
		public void onComplete() {
			child.onComplete();
		}

		private static byte[] getByteArray(final List<Byte> list) {
			final byte[] bytes = new byte[list.size()];
			for (int i = 0; i < list.size(); i++) {
				bytes[i] = list.get(i);
			}
			return bytes;
		}

		@Override
		public void dispose() {
			d.dispose();
		}

		@Override
		public boolean isDisposed() {
			return d.isDisposed();
		}

	}
}
