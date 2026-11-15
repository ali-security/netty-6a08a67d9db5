/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DelegatingDecompressorFrameListener}.
 */
public class DelegatingDecompressorFrameListenerTest {
    private static final int STREAM_ID = 3;

    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;

    @BeforeEach
    public void setUp() throws Http2Exception {
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        // Use an unpooled allocator so the reference count of released buffers stays observable.
        channel.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);
        ctx = channel.pipeline().firstContext();

        connection = new DefaultHttp2Connection(true);
        connection.local().flowController(mock(Http2LocalFlowController.class));
        connection.remote().createStream(STREAM_ID, false);
    }

    @AfterEach
    public void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    public void decompressedBufferReleasedWhenListenerThrows() throws Exception {
        final List<ByteBuf> received = new ArrayList<ByteBuf>();
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                                  boolean endOfStream) throws Http2Exception {
                if (!data.isReadable()) {
                    return padding;
                }
                received.add(data);
                throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR, "listener failure");
            }
        };

        assertDecompressedBuffersReleased(delegate, received);
    }

    @Test
    public void decompressedBufferReleasedWhenListenerThrowsRuntimeException() throws Exception {
        final List<ByteBuf> received = new ArrayList<ByteBuf>();
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                                  boolean endOfStream) {
                if (!data.isReadable()) {
                    return padding;
                }
                received.add(data);
                throw new IllegalStateException("listener failure");
            }
        };

        assertDecompressedBuffersReleased(delegate, received);
    }

    @Test
    public void decompressedBufferReleasedWhenFlowControlConsumeThrows() throws Exception {
        final List<ByteBuf> received = new ArrayList<ByteBuf>();
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                                  boolean endOfStream) {
                if (!data.isReadable()) {
                    return padding;
                }
                received.add(data);
                // Return more bytes than were decompressed, which makes the flow controller conversion fail.
                return data.readableBytes() + padding + 1;
            }
        };

        assertDecompressedBuffersReleased(delegate, received);
    }

    private void assertDecompressedBuffersReleased(Http2FrameListener delegate, List<ByteBuf> received)
            throws Exception {
        final DelegatingDecompressorFrameListener listener =
                new DelegatingDecompressorFrameListener(connection, delegate, 0);

        Http2Headers headers = new DefaultHttp2Headers()
                .set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
        listener.onHeadersRead(ctx, STREAM_ID, headers, 0, false);
        assertFalse(headers.contains(HttpHeaderNames.CONTENT_ENCODING));

        final ByteBuf compressed = Unpooled.wrappedBuffer(gzip("hello world, hello world, hello world"));
        try {
            assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    listener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
                }
            });
        } finally {
            compressed.release();
        }

        assertFalse(received.isEmpty());
        for (ByteBuf buf : received) {
            assertEquals(0, buf.refCnt());
        }

        connection.stream(STREAM_ID).close();
    }

    private static byte[] gzip(String content) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        try {
            gzip.write(content.getBytes(CharsetUtil.UTF_8.name()));
        } finally {
            gzip.close();
        }
        return out.toByteArray();
    }
}
