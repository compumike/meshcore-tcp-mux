require "./spec_helper"
require "../src/meshcore_tcp_mux/config"

describe MeshCoreTCPMux::Config do
  # Config keeps internal safety policies injectable for deterministic specs,
  # while validation prevents invalid combinations from reaching the runtime.
  it "accepts the default policy" do
    config = MeshCoreTCPMux::Config.new
    config.validate!
    config.command_age.should eq(15.seconds)
    config.virtual_sync_timeout.should eq(15.seconds)
  end

  it "rejects a nonpositive virtual sync deadline" do
    config = MeshCoreTCPMux::Config.new
    config.virtual_sync_timeout = Time::Span.zero
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
