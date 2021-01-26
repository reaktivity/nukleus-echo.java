/**
 * Copyright 2016-2021 The Reaktivity Project
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

import java.util.function.LongUnaryOperator;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.echo.internal.EchoConfiguration;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public final class EchoServerFactoryBuilder implements StreamFactoryBuilder
{
    private final EchoConfiguration config;

    private RouteManager router;
    private MutableDirectBuffer writeBuffer;
    private LongUnaryOperator supplyReplyId;

    public EchoServerFactoryBuilder(
        EchoConfiguration config)
    {
        this.config = config;
    }

    @Override
    public EchoServerFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public EchoServerFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        return new EchoServerFactory(
                config,
                router,
                writeBuffer,
                supplyReplyId);
    }
}
