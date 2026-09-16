require "./spec_helper"
require "../src/meshcore_tcp_mux/config"

describe MeshCoreTCPMux::Config do
  # Config keeps internal safety policies injectable for deterministic specs,
  # while validation prevents invalid combinations from reaching the runtime.
  it "accepts the default policy" do
    config = MeshCoreTCPMux::Config.new
    config.validate!
    config.listen_multi_client_port.should eq(5001)
    config.listen_dedicated_client_ports.should be_empty
    config.offline_queue_size.should eq(256)
    config.command_age.should eq(15.seconds)
    config.virtual_sync_timeout.should eq(15.seconds)
    config.radio_uncertainty_timeout.should eq(60.seconds)
  end

  it "accepts any number of unique dedicated-client ports" do
    config = MeshCoreTCPMux::Config.new
    config.listen_dedicated_client_ports = (5002..5102).to_a
    config.validate!
  end

  it "rejects duplicate ports across listener kinds" do
    config = MeshCoreTCPMux::Config.new
    config.listen_dedicated_client_ports = [5002, 5001]
    expect_raises(ArgumentError, /listen ports must be unique/) { config.validate! }
  end

  it "rejects an invalid dedicated port or offline queue size" do
    config = MeshCoreTCPMux::Config.new
    config.listen_dedicated_client_ports = [65_536]
    expect_raises(ArgumentError, /between 1 and 65535/) { config.validate! }

    config.listen_dedicated_client_ports = [5002]
    config.offline_queue_size = 0
    expect_raises(ArgumentError, /queue budgets must be positive/) { config.validate! }
  end

  it "rejects a nonpositive virtual sync deadline" do
    config = MeshCoreTCPMux::Config.new
    config.virtual_sync_timeout = Time::Span.zero
    expect_raises(ArgumentError, /deadlines/) { config.validate! }
  end

  it "rejects a nonpositive unknown-radio-acceptance quarantine" do
    config = MeshCoreTCPMux::Config.new
    config.radio_uncertainty_timeout = Time::Span.zero
    expect_raises(ArgumentError, /deadlines/) { config.validate! }
  end

  it "rejects nonpositive count limits" do
    config = MeshCoreTCPMux::Config.new
    config.output_frames = 0

    expect_raises(ArgumentError, /queue budgets must be positive/) do
      config.validate!
    end
  end

  it "rejects a contacts total timeout shorter than its idle timeout" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 6.seconds
    config.contacts_timeout = 5.seconds

    expect_raises(ArgumentError, /contacts total timeout/) do
      config.validate!
    end
  end
end
