/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.mesalab.executor.druid.granularity;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class GranularityModule extends SimpleModule
{

  public GranularityModule()
  {
    super("GranularityModule");

    setMixInAnnotation(Granularity.class, GranularityMixin.class);
    registerSubtypes(
        new NamedType(PeriodGranularity.class, "period"),
        new NamedType(DurationGranularity.class, "duration"),
        new NamedType(AllGranularity.class, "all"),
        new NamedType(NoneGranularity.class, "none")
    );

    addDeserializer(
            DateTimeZone.class,
            new JsonDeserializer<DateTimeZone>()
            {
              @Override
              public DateTimeZone deserialize(JsonParser jp, DeserializationContext ctxt)
                      throws IOException
              {
                String tzId = jp.getText();
                return DateTimes.inferTzFromString(tzId);
              }
            }
    );
    addSerializer(
            DateTimeZone.class,
            new JsonSerializer<DateTimeZone>()
            {
              @Override
              public void serialize(
                      DateTimeZone dateTimeZone,
                      JsonGenerator jsonGenerator,
                      SerializerProvider serializerProvider
              ) throws IOException
              {
                jsonGenerator.writeString(dateTimeZone.getID());
              }
            }
    );
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Granularity.class)
  public interface GranularityMixin
  {
  }
}
