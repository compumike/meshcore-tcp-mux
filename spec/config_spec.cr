require "./spec_helper"

describe MeshCoreTCPMux::Config do
  # Config keeps internal safety policies injectable for deterministic specs,
  # while validation prevents invalid combinations from reaching the runtime.
  it "accepts the default policy" do
    MeshCoreTCPMux::Config.new.validate!
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
