# Startup fencing discards stale pre-boundary traffic: five consecutive
# SELF_INFO replies exceed the firmware's four retained-response slots. A fresh
# DEVICE_INFO and acknowledged default-scope reset are then required for ready.
# These fixtures are decoded payloads, sometimes opcode-only stale markers.

require "./spec_helper"
require "../src/meshcore_tcp_mux/startup"

private def self_reply
  # SELF_INFO: minimum 58-byte response; opcode 5, public key occupies offsets 4..35.
  bytes = Bytes.new(58, 0)
  bytes[0] = 5
  bytes
end

private def device_reply
  # DEVICE_INFO: exactly 82 bytes, opcode 0x0d, protocol version 13 at offset 1; other fields
  # are synthetic.
  bytes = Bytes.new(82, 0)
  bytes[0] = 0x0d
  bytes[1] = 13
  bytes
end

describe MeshCoreTCPMux::Startup do
  it "constructs five correctly reserved app starts and the target-13 query" do
    probes = MeshCoreTCPMux::Startup.probes
    probes.size.should eq(6)
    # APP_START (1) followed by its seven required reserved zero bytes.
    probes.first[0, 8].should eq(Bytes[1, 0, 0, 0, 0, 0, 0, 0])
    String.new(probes.first[8..]).should eq("meshcore-mux")
    # DEVICE_QUERY (22), requested protocol target 13.
    probes.last.should eq(Bytes[0x16, 13])
  end

  it "cannot accept any stale prefix of up to four ordinary marker classes" do
    # OK (0x00): command accepted, not proof of radio delivery.
    # Opcode-only stale contacts marker; startup discards it before the synchronization
    # boundary.
    markers = [self_reply, device_reply, Bytes[0], Bytes[3], Bytes[4]]
    (0..4).each do |length|
      (markers.size ** length).times do |combination|
        startup = MeshCoreTCPMux::Startup.new(0.seconds)
        index = combination
        length.times do
          startup.receive(markers[index % markers.size], 0.seconds).should be_nil
          startup.ready?.should be_false
          index //= markers.size
        end
        # The first new app-start stops a stale contacts iterator.
        5.times do
          # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
          startup.receive(Bytes[0x83], 0.seconds).should be_nil
          startup.receive(self_reply, 0.seconds).should be_nil
        end
        # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default
        # scope.
        startup.receive(device_reply, 0.seconds).should eq(Bytes[0x36, 0])
        startup.ready?.should be_false
        # OK (0x00): command accepted, not proof of radio delivery.
        startup.receive(Bytes[0], 0.seconds)
        startup.ready?.should be_true
      end
    end
  end

  it "requires consecutive self replies and a confirmed scope reset" do
    startup = MeshCoreTCPMux::Startup.new(0.seconds)
    4.times { startup.receive(self_reply, 0.seconds) }
    # Opcode-only stale contacts marker; startup discards it before the synchronization
    # boundary.
    startup.receive(Bytes[3], 0.seconds)
    startup.receive(self_reply, 0.seconds)
    startup.receive(device_reply, 0.seconds).should be_nil
    expect_raises(MeshCoreTCPMux::Startup::Error, /timeout/) { startup.check_deadline(15.seconds) }
  end

  it "rejects unsupported profiles and a failed reset" do
    startup = MeshCoreTCPMux::Startup.new(0.seconds)
    5.times { startup.receive(self_reply, 0.seconds) }
    bad = device_reply
    bad[1] = 12
    expect_raises(MeshCoreTCPMux::Startup::Error) { startup.receive(bad, 0.seconds) }
    startup = MeshCoreTCPMux::Startup.new(0.seconds)
    5.times { startup.receive(self_reply, 0.seconds) }
    startup.receive(device_reply, 0.seconds)
    # ERR (0x01), UNSUPPORTED_CMD.
    expect_raises(MeshCoreTCPMux::Startup::Error, /scope/) { startup.receive(Bytes[1, 1], 0.seconds) }
  end
end
