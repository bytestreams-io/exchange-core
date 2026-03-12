package io.bytestreams.exchange.core;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.assertj.AttributeAssertion;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

class TestFixture {

  static final MessageReader<String> FRAMED_READER = TestFixture::readFramed;
  static final MessageWriter<String> FRAMED_WRITER = TestFixture::writeFramed;
  static final AttributeKey<String> CHANNEL_TYPE = OTel.CHANNEL_TYPE;
  static final AttributeKey<String> DIRECTION = OTel.DIRECTION;
  static final AttributeKey<String> MESSAGE_TYPE = OTel.MESSAGE_TYPE;
  static final AttributeKey<String> ERROR_TYPE = OTel.ERROR_TYPE;

  static void writeFramed(String msg, OutputStream out) throws IOException {
    byte[] data = msg.getBytes(StandardCharsets.UTF_8);
    int len = data.length;
    out.write(new byte[] {(byte) (len >> 24), (byte) (len >> 16), (byte) (len >> 8), (byte) len});
    out.write(data);
    out.flush();
  }

  static String readFramed(InputStream in) throws IOException {
    byte[] lenBuf = in.readNBytes(4);
    if (lenBuf.length < 4) {
      throw new IOException("Unexpected end of stream");
    }
    int len =
        ((lenBuf[0] & 0xFF) << 24)
            | ((lenBuf[1] & 0xFF) << 16)
            | ((lenBuf[2] & 0xFF) << 8)
            | (lenBuf[3] & 0xFF);
    byte[] data = in.readNBytes(len);
    if (data.length < len) {
      throw new IOException("Unexpected end of stream");
    }
    return new String(data, StandardCharsets.UTF_8);
  }

  static void assertLongSum(
      InMemoryMetricReader reader, String name, long value, AttributeAssertion... attrs) {
    assertThat(reader.collectAllMetrics())
        .anySatisfy(
            m ->
                assertThat(m)
                    .hasName(name)
                    .hasLongSumSatisfying(
                        sum ->
                            sum.hasPointsSatisfying(
                                point -> point.hasValue(value).hasAttributesSatisfying(attrs))));
  }

  static void assertHistogram(
      InMemoryMetricReader reader, String name, long count, AttributeAssertion... attrs) {
    assertThat(reader.collectAllMetrics())
        .anySatisfy(
            m ->
                assertThat(m)
                    .hasName(name)
                    .hasHistogramSatisfying(
                        hist ->
                            hist.hasPointsSatisfying(
                                point -> point.hasCount(count).hasAttributesSatisfying(attrs))));
  }
}
