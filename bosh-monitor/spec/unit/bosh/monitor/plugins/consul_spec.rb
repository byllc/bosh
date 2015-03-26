require 'spec_helper'

describe Bhm::Plugins::Consul do

  subject{ described_class.new(options)  }
  let(:heartbeat){ make_heartbeat(timestamp: 1320196099) }
  let(:uri){ URI.parse("http://fake-consul-cluster:8500/v1/event/fire/mysql_node_heartbeat?") }
  let(:request){ { :body => heartbeat.to_json } }
  let(:ttl_request){ { :body => heartbeat.to_json } }
  let(:heartbeat_name){ "test_" + heartbeat.job}
  let(:ttl_uri){ URI.parse("http://fake-consul-cluster:8500/v1/agent/check/#{heartbeat_name}?") }
  let(:register_uri){ URI.parse("http://fake-consul-cluster:8500/v1/agent/check/register?") }
  let(:register_request){ { :body => { "name" => "test_mysql_node", "notes" => "test", "ttl" => "120s"}.to_json } }


  describe "validating the options" do
    context "when we specify cluster_address, endpoint and port" do
      let(:options){ { 'cluster_address' => "fake-consul-cluster", 'events_api' => '/v1/api', 'port' => 8500 } }
      it "is valid" do
        subject.run
        expect(subject.validate_options).to eq(true)
      end
    end

    context "when we omit the cluster address" do
      let(:options){ {'cluster_address' => nil} }
      it "is not valid" do
        subject.run
        expect(subject.validate_options).to eq(false)
      end
    end

    context "when we omit the enpoint and port" do
      let(:options){ {'cluster_address' => 'fake-consul-cluster'} }
      it "is valid" do
        subject.run
        expect(subject.validate_options).to eq(true)
      end
    end

  end


  describe "forwarding event messages to consul" do

    context "without valid options" do
      let(:options){ { 'cluster_address' => nil } }
      it "it should not forward events if options are invalid" do
        subject.run
        expect(subject).to_not receive(:send_http_put_request).with(uri, request)
        subject.process(heartbeat)
      end
    end

    context "with valid options" do
      let(:options){ { 'cluster_address' => 'fake-consul-cluster', 'events' => true} }
      it "should successully hand the event off to http forwarder" do
        subject.run
        expect(subject).to receive(:send_http_put_request).with(uri, request)
        subject.process(heartbeat)
      end
    end
  end

  describe "sending ttl requests to consul" do
    let(:options){ { 'cluster_address' => 'fake-consul-cluster', 'ttl' => "120s", 'namespace' => 'test_', 'ttl_note' => 'test'} }


    it "should send a put request to the register endpoint the first time an event is encountered" do
      subject.run
      expect(subject).to receive(:send_http_put_request).with(register_uri, register_request)
      subject.process(heartbeat)
    end

    it "should send a put request to the ttl endpoint the second time an event is encountered" do
      EM.run do
        subject.run
        subject.process(heartbeat)
        expect(subject).to receive(:send_http_put_request).with(ttl_uri, ttl_request)
        subject.process(heartbeat)
        EM.stop
      end
    end

    it "should not send a registration request if an event is already registered" do
      subject.run
      EM.run do
        subject.process(heartbeat)
        EM.stop
      end

      expect(subject).to_not receive(:send_http_put_request).with(register_uri, register_request)
      subject.process(heartbeat)
    end


    describe "when events are also enabled" do
      let(:options){ { 'cluster_address' => 'fake-consul-cluster', 'ttl' => "120s", 'events' => true, 'namespace' => 'test_', 'ttl_note' => 'test'} }

      it "should send ttl and event requests in a single loop" do
        subject.run

        EM.run do
          subject.process(heartbeat)
          EM.stop
        end
        expect(subject).to receive(:send_http_put_request).with(uri, request)
        expect(subject).to receive(:send_http_put_request).with(ttl_uri, ttl_request)
        subject.process(heartbeat)
      end
    end
  end

end
