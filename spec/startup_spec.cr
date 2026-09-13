require "./spec_helper"
require "../src/meshcore_tcp_mux/startup"

private def self_reply
  bytes = Bytes.new(58, 0)
  bytes[0] = 5
  bytes
end

private def device_reply
  bytes = Bytes.new(82, 0)
  bytes[0] = 0x0d
  bytes[1] = 13
  bytes
end

describe MeshCoreTCPMux::Startup do
  it "constructs five correctly reserved app starts and the target-13 query" do
    probes = MeshCoreTCPMux::Startup.probes
    probes.size.should eq(6)
    probes.first[0, 8].should eq(Bytes[1, 0, 0, 0, 0, 0, 0, 0])
    String.new(probes.first[8..]).should eq("meshcore-mux")
    probes.last.should eq(Bytes[0x16, 13])
  end

  it "cannot accept any stale prefix of up to four ordinary marker classes" do
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
          startup.receive(Bytes[0x83], 0.seconds).should be_nil
          startup.receive(self_reply, 0.seconds).should be_nil
        end
        startup.receive(device_reply, 0.seconds).should eq(Bytes[0x36, 0])
        startup.ready?.should be_false
        startup.receive(Bytes[0], 0.seconds)
        startup.ready?.should be_true
      end
    end
  end

  it "requires consecutive self replies and a confirmed scope reset" do
    startup = MeshCoreTCPMux::Startup.new(0.seconds)
    4.times { startup.receive(self_reply, 0.seconds) }
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
    expect_raises(MeshCoreTCPMux::Startup::Error, /scope/) { startup.receive(Bytes[1, 1], 0.seconds) }
  end
end
