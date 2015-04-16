# Consul Bosh Monitor Plugin
# Forwards alert and heartbeat messages as events to a consul cluster
module Bosh::Monitor
  module Plugins
    class ConsulEventForwarder < Base
      include Bosh::Monitor::Plugins::HttpRequestHelper

      DEFAULT_ENDPOINT       = '/v1/event/fire/'
      DEFAULT_TTL_ENDPOINT   = '/v1/agent/check/'
      DEFAULT_PORT           = '8500'
      DEFAULT_PROTOCOL       = 'http'
      DEFAULT_TTL_REG_PATH   = "register"
      DEFAULT_TTL_NOTE       = "Automatically Registered by BOSH-MONITOR"
      CONSUL_REQUEST_HEADER  = { 'Content-Type' => 'application/javascript' }

      def run
        @checklist       = []
        @cluster_address = options["cluster_address"] || ""
        @namespace       = options['namespace']       || ""
        @events_api      = options["events_api"]      || DEFAULT_ENDPOINT
        @ttl_api         = options["ttl_api"]         || DEFAULT_TTL_ENDPOINT
        @port            = options["port"]            || DEFAULT_PORT
        @protocol        = options["protocal"]        || DEFAULT_PROTOCOL
        @params          = options["params"]
        @ttl             = options['ttl']
        @use_events      = options['events']          || false
        @ttl_note        = options['ttl_note']        || DEFAULT_TTL_NOTE

        @ttl_register_path  = @ttl_api + DEFAULT_TTL_REG_PATH
        @use_ttl            = !@ttl.nil?
      end

      def validate_options
        !@cluster_address.empty?
      end

      def process(event)
        validate_options && forward_event(event)
      end

      private

      def consul_uri(event, note_type)
        path = get_path_for_note_type(event, note_type)
        URI.parse("#{@protocol}://#{@cluster_address}:#{@port}#{path}?#{@params}")
      end

      def forward_event(event)
        notify_consul(event, :event)  if @use_events

        if event_unregistered?(event)
          notify_consul(event, :register, registration_payload(event))
        elsif @use_ttl
          notify_consul(event, :ttl)
        end
      end

      def get_path_for_note_type(event, note_type)
        case note_type
        when :event
          @events_api + label_for_event(event)
        when :ttl
          @ttl_api + label_for_ttl(event)
        when :register
          @ttl_register_path
        end
      end

      def label_for_event(event)
        case event
          when Bosh::Monitor::Events::Heartbeat
            "#{event.job}_heartbeat"
          when Bosh::Monitor::Events::Alert
            event_label = event.title.downcase.gsub(" ","_")
            "#{event_label}_alert"
          else
            "event"
        end
      end

      def label_for_ttl(event)
        "#{@namespace}#{event.job}"
      end

      # Notify consul of an event
      # note_type: teh type of notice we are sending (:event, :ttl, :register)
      # message:   an optional body for the message, event.json is used by default
      def notify_consul(event, note_type, message=nil)
        body    = message.nil? ? event.to_json : message.to_json
        uri     = consul_uri(event, note_type)
        request = { :body => body }
        send_http_put_request(uri , request)

        #if a registration request returns without error we log it
        #we don't want to send extra registrations
        @checklist << event.job if note_type == :register
        true
      rescue => e
        logger.info("Could not forward event to Consul Cluster @#{@cluster_address}: #{e.inspect}")
        false
      end

      #Has this process not encountered a specific ttl check yet?
      #We keep track so we aren't sending superfluous registrations
      def event_unregistered?(event)
        @use_ttl && !@checklist.include?(event.job)
      end

      def registration_payload(event)
        name = "#{@namespace}#{event.job}"
        { "name"  => name, "notes" => @ttl_note, "ttl" => @ttl }
      end

    end
  end
end
