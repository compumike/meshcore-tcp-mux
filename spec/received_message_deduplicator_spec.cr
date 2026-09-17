require "./spec_helper"
require "../src/meshcore_tcp_mux/received_message_deduplicator"

describe MeshCoreTCPMux::ReceivedMessageDeduplicator do
  # Retry identity is logical rather than byte-for-byte: legacy and V3 receive
  # envelopes can describe the same text despite different receive metadata.
  it "deduplicates direct text by sender, type, timestamp, and contents" do
    deduplicator = MeshCoreTCPMux::ReceivedMessageDeduplicator.new

    # CONTACT_MESSAGE (0x07): six-byte synthetic sender prefix, direct-path
    # sentinel, plain-text type, timestamp 0x04030201 little-endian, then text.
    legacy = Bytes[
      0x07_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8,
      0xff_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8,
    ] + "hello".to_slice
    deduplicator.duplicate?(legacy).should be_false

    # CONTACT_MESSAGE_V3 (0x10): SNR/reserved prefix precedes the same sender;
    # flood path metadata differs, but retry attempts are absent from this
    # companion frame and all logical message fields remain identical.
    v3_retry = Bytes[
      0x10_u8, 0xf8_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8,
      3_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8,
    ] + "hello".to_slice
    deduplicator.duplicate?(v3_retry).should be_true

    other_sender = v3_retry.dup
    other_sender[4] = 9_u8
    deduplicator.duplicate?(other_sender).should be_false
  end

  it "deduplicates channel text without collapsing other channels or contents" do
    deduplicator = MeshCoreTCPMux::ReceivedMessageDeduplicator.new

    # CHANNEL_MESSAGE (0x08): channel two, flood path length one, plain-text
    # type, timestamp 0x04030201 little-endian, then sender-prefixed text.
    message = Bytes[
      0x08_u8, 2_u8, 1_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8,
    ] + "sender: hello".to_slice
    deduplicator.duplicate?(message).should be_false

    retry = message.dup
    retry[2] = 4_u8
    deduplicator.duplicate?(retry).should be_true

    other_channel = retry.dup
    other_channel[1] = 3_u8
    deduplicator.duplicate?(other_channel).should be_false

    other_text = retry.dup
    other_text[8] = 'S'.ord.to_u8
    deduplicator.duplicate?(other_text).should be_false
  end
end
