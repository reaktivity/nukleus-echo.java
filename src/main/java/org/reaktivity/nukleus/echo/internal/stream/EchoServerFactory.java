/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.echo.internal.stream;

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.echo.internal.EchoConfiguration;
import org.reaktivity.nukleus.echo.internal.types.OctetsFW;
import org.reaktivity.nukleus.echo.internal.types.control.RouteFW;
import org.reaktivity.nukleus.echo.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.echo.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.echo.internal.types.stream.DataFW;
import org.reaktivity.nukleus.echo.internal.types.stream.EndFW;
import org.reaktivity.nukleus.echo.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.echo.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class EchoServerFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTrace;

    private final MessageFunction<RouteFW> wrapRoute;

    public EchoServerFactory(
        EchoConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTrace)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.wrapRoute = this::wrapRoute;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long initialId = begin.streamId();

            newStream = new EchoServer(
                    sender,
                    routeId,
                    initialId)::onStream;
        }

        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class EchoServer
    {
        private final MessageConsumer receiver;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private EchoServer(
            MessageConsumer receiver,
            long routeId,
            long initialId)
        {
            this.receiver = receiver;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
        }

        private void onStream(
            final int msgTypeId,
            final DirectBuffer buffer,
            final int index,
            final int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            default:
                doReset(receiver, routeId, initialId, supplyTrace.getAsLong());
                break;
            }
        }

        private void onThrottle(
            final int msgTypeId,
            final DirectBuffer buffer,
            final int index,
            final int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            final BeginFW begin)
        {
            final long traceId = begin.trace();

            router.setThrottle(replyId, this::onThrottle);
            doBegin(receiver, routeId, replyId, traceId);
        }

        private void onData(
            final DataFW data)
        {
            final long traceId = data.trace();
            final int flags = data.flags();
            final long groupId = data.groupId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            doData(receiver, routeId, replyId, traceId, flags, groupId, reserved, payload, extension);
        }

        private void onEnd(
            final EndFW end)
        {
            final long traceId = end.trace();
            doEnd(receiver, routeId, replyId, traceId);
        }

        private void onAbort(
            final AbortFW abort)
        {
            final long traceId = abort.trace();
            doAbort(receiver, routeId, replyId, traceId);
        }

        private void onReset(
            final ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(receiver, routeId, initialId, traceId);
        }

        private void onWindow(
            final WindowFW window)
        {
            final long traceId = window.trace();
            final int credit = window.credit();
            final int padding = window.padding();
            final long groupId = window.groupId();

            doWindow(receiver, routeId, initialId, traceId, credit, padding, groupId);
        }
    }

    private void doBegin(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId,
        final int flags,
        final long groupId,
        final int reserved,
        final OctetsFW payload,
        final OctetsFW extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .flags(flags)
                .groupId(groupId)
                .reserved(reserved)
                .payload(payload)
                .extension(extension)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doAbort(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doEnd(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .trace(traceId)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
